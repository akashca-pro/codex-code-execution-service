/**
 * Enum representing events of redis.
 * 
 * @enum
 */
export enum RedisEvents {
    READY = 'ready',
    ERROR = 'error',
    CLOSE = 'close',
    RECONNECTING = 'reconnecting'
}