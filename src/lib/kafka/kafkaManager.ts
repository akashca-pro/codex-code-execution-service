import { Kafka, Producer, Consumer, Admin, logLevel, Message } from 'kafkajs';
import logger from '@/utils/logger';
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
    if (!this.producer) {
      this.producer = this.kafka.producer({ idempotent: true });
      await this.producer.connect();
    }
    if (!this.admin) {
      this.admin = this.kafka.admin();
      await this.admin.connect();
    }

    logger.info('KafkaManager initialized');
  }

  // Create topic if not exists
  public async createTopic(topic: string, numPartitions: number = 1, replicationFactor: number = 1) {
    if (!this.admin) throw new Error('Kafka admin not initialized');
    try {
      await this.admin.createTopics({
        topics: [{ topic, numPartitions, replicationFactor }],
        waitForLeaders: true, 
      });
      logger.info(`Created topic: ${topic}`);
    } catch (error: any) {
      if (error.code === 36) {
        logger.info(`Topic "${topic}" already exists. Skipping creation.`);
      } else {
        throw error; 
      }
    }
  }

  // Send message with retry metadata
  public async sendMessage(topic: string, key: string | null, value: any, headers?: Record<string, string>) {
    if (!this.producer) throw new Error('Kafka producer not initialized');
    
    const finalValue = typeof value === 'string' ? value : JSON.stringify(value);

    await this.producer.send({
      topic,
      messages: [{ key, value: finalValue, headers: headers || {} }]
    });
  }


  // Batch message sending
  public async sendBatch(topic: string, messages: { key: string | null, value: any }[]) {
    if (!this.producer) throw new Error('Kafka producer not initialized');
    const kafkaMessages: Message[] = messages.map(m => ({
      key: m.key,
      value: JSON.stringify(m.value)
    }));
    await this.producer.send({ topic, messages: kafkaMessages });
  }

  // Consumer with retry + DLQ
  public async createConsumer(
    groupId: string, 
    topic: string, 
    eachMessage: (payload: any) => Promise<void>, 
    dlqTopic: string = this.dlqTopic) {
    if (this.consumers.has(groupId)) return this.consumers.get(groupId)!;
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

        let payload : any;
        //parse payload 
        try {
          payload = message.value ? JSON.parse(message.value.toString()) : null;
        } catch (parseError) {
          logger.error('Invalid JSON, sending to DLQ', { topic, partition, offset, key, parseError });
          await this.sendMessage(
            dlqTopic,
            message.key?.toString() || null,
            message.value?.toString(),
          );
          await consumer.commitOffsets([{ topic, partition, offset : (Number(offset) + 1).toString() }]);
          return; // skip retry
        }
        // process payload
        try {
          await eachMessage(payload);
          await consumer.commitOffsets([{ topic, partition, offset : (Number(offset) + 1).toString() }]);
        } catch (processingError) {
          logger.error('Message processing failed, scheduling retry', { topic, partition, offset, key, processingError });
          try {
            await this.scheduleRetry(topic, message, dlqTopic);
            await consumer.commitOffsets([{ topic, partition, offset: (Number(offset) + 1).toString() }]);
          } catch (retryError) {
            logger.error('Retry scheduling failed; not committing offset', { topic, partition, offset, key, retryError });
            throw retryError;
          }
        }
      }
    });

    this.consumers.set(groupId, consumer);
    logger.info(`Consumer created for groupId=${groupId}, topic=${topic}`);
    return consumer;
  }

  // Schedule retry in Redis
  private async scheduleRetry(topic: string, message: Message, dlqTopic: string) {
    const headers = message.headers || {};
    const retryCount = this.getHeaderString(message.headers, 'x-retry-count') ? parseInt(this.getHeaderString(message.headers, 'x-retry-count')!) : 0;

    if (retryCount >= this.maxRetries) {
      if (dlqTopic) {
        await this.sendMessage(
          dlqTopic, 
          message.key?.toString() || null, message.value?.toString(),
          { 'x-retry-count': String(retryCount) }
        );
        logger.warn(`Message moved to DLQ after ${this.maxRetries} retries`);
      }
      return;
    }

     const qSize = await redis.zcard(this.retryQueueKey);
     const cap = this.retryQueueCap;
     if(qSize >= cap){
      logger.error('Retry queue capacity reached, sending message to DLQ', { qSize, cap, topic });
      await this.sendMessage(
        dlqTopic,
        message.key?.toString() || null,
        message.value?.toString(),
        { 'x-error': 'retry-queue-capacity' }
      );
      return;
     }

    const now = Math.floor(Date.now() / 1000);
    const nextRetry = now + this.getRetryDelay(retryCount);
    const retryData = {
      topic,
      key: message.key?.toString() || null,
      value: message.value?.toString() || '',
      headers: { ...headers, 'x-retry-count': String(retryCount + 1) },
      firstSeen : headers['x-first-seen'] ?? String(now),
    };
    try {
      await redis.zadd(this.retryQueueKey, nextRetry, JSON.stringify(retryData));
    } catch (error) {
      throw error;
    }
  }

  // Retry worker
  public async startRetryWorker() {
    if (this.retryWorkerActive) return;
    this.retryWorkerActive = true;

    let processing = false;
    this.retryWorkerInterval = setInterval(async ()=> {
      if(processing) return
      processing = true;
      try {
        const batchSize = 50;
        const items = await redis.zpopmin(this.retryQueueKey, batchSize);

        if (!items || items.length === 0) return;

        for(let i = 0; i<items.length; i+= 2){
          const raw = items[i];
          try {
            const data = JSON.parse(raw);
            await this.sendMessage(data.topic, data.key, data.value, data.headers);
            // inc metric (retry)
          } catch (error) {
            logger.error('Retry worker failed for message', { raw, error });
            await this.sendMessage(this.dlqTopic, null, raw, { 'x-error': 'retry-worker-failed' });
          }
        }
      } catch (error) {
        logger.error('Retry worker tick failed', error);
      } finally {
        processing = false;
      }
    },5000);
  }

  // Dynamic Config Reload
  public async reloadConfig(newBrokers: string[]) {
    logger.info('Reloading Kafka brokers...');
    await this.disconnect();
    this.kafka = new Kafka({ clientId: config.KAFKA_CODE_EXECUTION_SERVICE_CLIENT_ID, brokers: newBrokers, logLevel: logLevel.INFO });
    await this.init();
  }

  // Graceful Shutdown
  public async disconnect(): Promise<void> {
    await Promise.all([...this.consumers.values()].map((c) => c.disconnect()));
    if (this.producer) await this.producer.disconnect();
    if (this.admin) await this.admin.disconnect();
    if (this.retryWorkerActive && this.retryWorkerInterval) {
      clearInterval(this.retryWorkerInterval);
      this.retryWorkerActive = false;
    }
    logger.info('KafkaManager disconnected');
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
