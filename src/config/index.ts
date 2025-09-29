interface Config {
    REDIS_URL : string;
    SERVICE_NAME : string; 
    METRICS_PORT : number;
    KAFKA_IDEMPOTENCY_KEY_EXPIRY : number;
    KAFKA_CLIENT_ID : string;
    KAFKA_BROKERS : string;
    KAFKA_GROUP_ID : string;
    KAFKA_MAX_RETRIES : number;
    KAFKA_RETRY_QUEUE_CAP : number;
    WORKER_IMAGE_NAME : string;
    EXECUTION_TIMEOUT : number;
    MAX_OUTPUT_LENGTH : number;
    CONTAINER_POOL_SIZE : number;
}

export const config : Config = {
    REDIS_URL : process.env.REDIS_URL || '',
    METRICS_PORT : Number(process.env.METRICS_PORT) || 9104,
    SERVICE_NAME : process.env.SERVICE_NAME || 'CODE_EXECUTION_SERVICE',
    KAFKA_IDEMPOTENCY_KEY_EXPIRY : Number(process.env.KAFKA_IDEMPOTENCY_KEY_EXPIRY),
    KAFKA_CLIENT_ID : process.env.KAFKA_CLIENT_ID || 'code-manage-service',
    KAFKA_BROKERS : process.env.KAFKA_BROKERS || 'localhost:9092',
    KAFKA_GROUP_ID : process.env.KAFKA_GROUP_ID!,
    KAFKA_MAX_RETRIES : Number(process.env.KAFKA_MAX_RETRIES)!,
    KAFKA_RETRY_QUEUE_CAP : Number(process.env.KAFKA_RETRY_QUEUE_CAP)!,
    WORKER_IMAGE_NAME : process.env.WORKER_IMAGE_NAME!,
    EXECUTION_TIMEOUT : Number(process.env.EXECUTION_TIMEOUT),
    MAX_OUTPUT_LENGTH : Number(process.env.MAX_OUTPUT_LENGTH),
    CONTAINER_POOL_SIZE : Number(process.env.CONTAINER_POOL_SIZE)
}