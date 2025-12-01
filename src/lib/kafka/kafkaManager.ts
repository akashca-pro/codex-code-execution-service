import { Kafka, Producer, Consumer, Admin, logLevel, Message } from 'kafkajs';
import logger from '@/utils/pinoLogger';
import { config } from '@/config';
import redis from '@/config/redis';
import { KafkaTopics } from './kafkaTopics';

export class KafkaManager {
  private static _instance: KafkaManager;
  private kafka: Kafka;
  private producer?: Producer;
  private admin?: Admin;
  private consumers = new Map<string, Consumer>();
  private retryQueueKey = KafkaTopics.RETRY_QUEUE;
  private dlqTopic = KafkaTopics.DLQ_QUEUE;
  private maxRetries = config.KAFKA_MAX_RETRIES;
  private retryQueueCap = config.KAFKA_RETRY_QUEUE_CAP;
  private retryWorkerActive = false;
  private retryWorkerInterval?: NodeJS.Timeout;


  private constructor() {
    this.kafka = new Kafka({
      clientId: config.KAFKA_CODE_EXECUTION_SERVICE_CLIENT_ID,
      brokers: config.KAFKA_BROKERS.split(','),
      logLevel: logLevel.INFO,
    });

    logger.info('KafkaManager instance created', { clientId: config.KAFKA_CODE_EXECUTION_SERVICE_CLIENT_ID, brokers: config.KAFKA_BROKERS });

    // Graceful shutdown
    process.on('SIGINT', () => this.disconnect());
    process.on('SIGTERM', () => this.disconnect());
  }

  public static getInstance(): KafkaManager {
    if (!KafkaManager._instance) {
      KafkaManager._instance = new KafkaManager();
    }
    return KafkaManager._instance;
  }

  public async init(): Promise<void> {
    logger.info('Initializing KafkaManager connections...');

    if (!this.producer) {
      this.producer = this.kafka.producer({ idempotent: true });
      await this.producer.connect();
      logger.info('Kafka Producer connected');
    }
    if (!this.admin) {
      this.admin = this.kafka.admin();
      await this.admin.connect();
      logger.info('Kafka Admin connected');
    }

    logger.info('KafkaManager initialization complete');
  }

  // Create topic if not exists
  public async createTopic(topic: string, numPartitions: number = 1, replicationFactor: number = 1) {
    if (!this.admin) {
      logger.error('Attempted to create topic before Kafka admin initialization', { topic });
      throw new Error('Kafka admin not initialized');
    }
    try {
      logger.debug(`Attempting to create topic: ${topic}`);
      await this.admin.createTopics({
        topics: [{ topic, numPartitions, replicationFactor }],
        waitForLeaders: true, 
      });
      logger.info(`Kafka topic created successfully: ${topic}`);
    } catch (error: any) {
      if (error.code === 36) {
        logger.info(`Kafka topic "${topic}" already exists. Skipping creation.`);
      } else {
        logger.error(`Failed to create Kafka topic: ${topic}`, { error });
        throw error; 
      }
    }
  }

  // Send message with retry metadata
  public async sendMessage(topic: string, key: string | null, value: any, headers?: Record<string, string>) {
    if (!this.producer) {
      logger.error('Attempted to send message before Kafka producer initialization', { topic, key });
      throw new Error('Kafka producer not initialized');
    }
    
    const finalValue = typeof value === 'string' ? value : JSON.stringify(value);

    try {
      await this.producer.send({
        topic,
        messages: [{ key, value: finalValue, headers: headers || {} }]
      });
      logger.debug(`Message sent to topic: ${topic}`, { key, headers });
    } catch (error) {
      logger.error(`Failed to send message to topic: ${topic}`, { key, error });
      throw error;
    }
  }


  // Batch message sending
  public async sendBatch(topic: string, messages: { key: string | null, value: any }[]) {
    if (!this.producer) {
      logger.error('Attempted to send batch before Kafka producer initialization', { topic, count: messages.length });
      throw new Error('Kafka producer not initialized');
    }
    const kafkaMessages: Message[] = messages.map(m => ({
      key: m.key,
      value: JSON.stringify(m.value)
    }));
    try {
      await this.producer.send({ topic, messages: kafkaMessages });
      logger.info(`Batch of ${messages.length} messages sent to topic: ${topic}`);
    } catch (error) {
      logger.error(`Failed to send batch to topic: ${topic}`, { count: messages.length, error });
      throw error;
    }
  }

  // Consumer with retry + DLQ
  public async createConsumer(
    groupId: string, 
    topic: string, 
    eachMessage: (payload: any) => Promise<void>, 
    dlqTopic: string = this.dlqTopic) {
    
    if (this.consumers.has(groupId)) {
        logger.warn(`Consumer already exists for groupId: ${groupId}. Returning existing instance.`);
        return this.consumers.get(groupId)!;
    }
    
    logger.info(`Creating Kafka consumer`, { groupId, topic, dlqTopic });
    const consumer = this.kafka.consumer({ 
        groupId,
    });

    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: false });

    await consumer.run({
      autoCommit : false,

      eachMessage: async ({ topic, partition, message }) => {
        const offset = message.offset;
        const key = message.key?.toString() || null;
        logger.debug(`[CONSUMER: ${groupId}] Received message`, { topic, partition, offset, key });

        let payload : any;
        //parse payload 
        try {
          payload = message.value ? JSON.parse(message.value.toString()) : null;
        } catch (parseError) {
          logger.error(`[CONSUMER: ${groupId}] Invalid JSON payload. Sending to DLQ.`, { topic, partition, offset, key, parseError });
          await this.sendMessage(
            dlqTopic,
            message.key?.toString() || null,
            message.value?.toString(),
            { 'x-error': 'invalid-json' }
          );
          await consumer.commitOffsets([{ topic, partition, offset : (Number(offset) + 1).toString() }]);
          return; // skip retry
        }
        
        // process payload
        try {
          await eachMessage(payload);
          await consumer.commitOffsets([{ topic, partition, offset : (Number(offset) + 1).toString() }]);
          logger.debug(`[CONSUMER: ${groupId}] Message processed successfully and offset committed.`, { topic, offset, key });
        } catch (processingError) {
          logger.error(`[CONSUMER: ${groupId}] Message processing failed. Scheduling retry.`, { topic, partition, offset, key, processingError });
          try {
            await this.scheduleRetry(topic, message, dlqTopic);
            await consumer.commitOffsets([{ topic, partition, offset: (Number(offset) + 1).toString() }]);
            logger.debug(`[CONSUMER: ${groupId}] Message retry scheduled and offset committed.`, { topic, offset, key });
          } catch (retryError) {
            logger.error(`[CONSUMER: ${groupId}] Retry scheduling failed; NOT committing offset to allow redelivery.`, { topic, partition, offset, key, retryError });
            // By throwing here, the consumer will stop processing and attempt to reconnect/redeliver,
            // effectively acting as a pause until the error is resolved.
            throw retryError; 
          }
        }
      }
    });

    this.consumers.set(groupId, consumer);
    logger.info(`Kafka Consumer successfully running for groupId=${groupId}, topic=${topic}`);
    return consumer;
  }

  // Schedule retry in Redis
  private async scheduleRetry(topic: string, message: Message, dlqTopic: string) {
    const key = message.key?.toString() || 'no-key';
    const headers = message.headers || {};
    const retryCountStr = this.getHeaderString(message.headers, 'x-retry-count');
    const retryCount = retryCountStr ? parseInt(retryCountStr) : 0;
    
    logger.debug('Scheduling message for retry', { topic, key, currentRetryCount: retryCount });

    if (retryCount >= this.maxRetries) {
      if (dlqTopic) {
        await this.sendMessage(
          dlqTopic, 
          message.key?.toString() || null, message.value?.toString(),
          { ...headers, 'x-retry-count': String(retryCount), 'x-error': 'max-retries-reached' }
        );
        logger.warn(`Message moved to DLQ after reaching max retries (${this.maxRetries}).`, { topic, key });
      }
      return;
    }

     const qSize = await redis.zcard(this.retryQueueKey);
     const cap = this.retryQueueCap;
     if(qSize >= cap){
      logger.error('Retry queue capacity reached. Sending message to DLQ.', { qSize, cap, topic, key });
      await this.sendMessage(
        dlqTopic,
        message.key?.toString() || null,
        message.value?.toString(),
        { 'x-error': 'retry-queue-capacity' }
      );
      return;
     }

    const now = Math.floor(Date.now() / 1000);
    const delay = this.getRetryDelay(retryCount);
    const nextRetry = now + delay;

    const retryData = {
      topic,
      key: key,
      value: message.value?.toString() || '',
      headers: { ...headers, 'x-retry-count': String(retryCount + 1) },
      firstSeen : this.getHeaderString(headers, 'x-first-seen') ?? String(now),
    };
    try {
      await redis.zadd(this.retryQueueKey, nextRetry, JSON.stringify(retryData));
      logger.info(`Message scheduled for retry in ${delay} seconds (Retry: ${retryCount + 1})`, { topic, key });
    } catch (error) {
      logger.error('Failed to add message to Redis retry queue.', { topic, key, error });
      throw error;
    }
  }

  // Retry worker
  public async startRetryWorker() {
    if (this.retryWorkerActive) {
      logger.warn('Attempted to start retry worker, but it is already active.');
      return;
    }
    this.retryWorkerActive = true;
    logger.info('Starting Kafka retry worker...');

    let processing = false;
    this.retryWorkerInterval = setInterval(async ()=> {
      if(processing) {
        logger.debug('Retry worker tick skipped (previous batch still processing).');
        return;
      }
      processing = true;
      try {
        const batchSize = 50;
        // ZPOPMIN is used here to atomically fetch and remove.
        const items = await redis.zpopmin(this.retryQueueKey, batchSize); 

        if (!items || items.length === 0) {
          logger.debug('Retry queue is empty or no items ready for processing.');
          return;
        }
        
        logger.info(`Retry worker processing batch of ${items.length / 2} messages.`);
        
        for(let i = 0; i<items.length; i+= 2){
          const raw = items[i];
          const score = items[i+1];
          try {
            const data = JSON.parse(raw);
            logger.debug('Retry worker resending message to original topic.', { originalTopic: data.topic, key: data.key, score });
            await this.sendMessage(data.topic, data.key, data.value, data.headers);
            // inc metric (retry) -> This part would be added in a real scenario
          } catch (error) {
            logger.error('Retry worker failed to process/resend message. Sending to DLQ.', { raw, score, error });
            await this.sendMessage(this.dlqTopic, null, raw, { 'x-error': 'retry-worker-resend-failed' });
          }
        }
        logger.info('Retry worker batch completed.');
      } catch (error) {
        logger.error('Retry worker main loop failed.', { error });
      } finally {
        processing = false;
      }
    },5000);
  }

  // Dynamic Config Reload
  public async reloadConfig(newBrokers: string[]) {
    logger.warn('Reloading Kafka brokers...');
    try {
      await this.disconnect();
      this.kafka = new Kafka({ clientId: config.KAFKA_CODE_EXECUTION_SERVICE_CLIENT_ID, brokers: newBrokers, logLevel: logLevel.INFO });
      await this.init();
      logger.info('Kafka brokers reloaded successfully.');
    } catch (error) {
      logger.error('Failed to reload Kafka configuration.', { error });
      throw error;
    }
  }

  // Graceful Shutdown
  public async disconnect(): Promise<void> {
    logger.info('Starting KafkaManager graceful shutdown...');
    
    // 1. Disconnect Consumers
    const consumerDisconnections = [...this.consumers.values()].map(async (c) => {
        const groupId = (c as any).groupId;
        logger.debug(`Disconnecting consumer: ${groupId}`);
        try {
            await c.disconnect();
            logger.info(`Consumer disconnected: ${groupId}`);
        } catch (error) {
            logger.error(`Error disconnecting consumer: ${groupId}`, { error });
        }
    });
    await Promise.all(consumerDisconnections);
    
    // 2. Clear Retry Worker
    if (this.retryWorkerActive && this.retryWorkerInterval) {
      clearInterval(this.retryWorkerInterval);
      this.retryWorkerActive = false;
      logger.info('Kafka Retry Worker stopped.');
    }
    
    // 3. Disconnect Producer
    if (this.producer) {
        await this.producer.disconnect();
        logger.info('Kafka Producer disconnected.');
    }
    
    // 4. Disconnect Admin
    if (this.admin) {
        await this.admin.disconnect();
        logger.info('Kafka Admin disconnected.');
    }

    logger.info('KafkaManager gracefully disconnected.');
  }

  private getRetryDelay(retryCount: number) {
    // base in seconds from config
    const base = Number(process.env.KAFKA_RETRY_BASE_SECONDS ?? 5);
    const max = Number(process.env.KAFKA_RETRY_MAX_SECONDS ?? 600);
    // exponential: base * 2^retryCount
    const exp = Math.min(base * Math.pow(2, retryCount), max);
    const jitter = Math.floor(Math.random() * Math.max(1, Math.round(exp * 0.1))); // 0-10% jitter
    return Math.round(exp + jitter);
  }

  private getHeaderString (h: any, key: string) {
    return h && h[key] ? (Buffer.isBuffer(h[key]) ? h[key].toString() : String(h[key])) : undefined;
  }
}

export const kafkaManager = KafkaManager.getInstance();