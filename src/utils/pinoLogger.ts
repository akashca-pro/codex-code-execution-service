import pino from 'pino';
import { context, trace } from '@opentelemetry/api';
import { config } from '@/config';

const logger = pino({
  level: process.env.NODE_ENV === 'production' ? 'info' : 'debug',
  base : {
    service : config.SERVICE_NAME
  },
  transport: process.env.NODE_ENV !== 'production'
    ? { target: 'pino-pretty', options: { colorize: true, translateTime: 'SYS:standard' } }
    : undefined,
});

// Helper to extract trace & span IDs
function getTraceContext() {
  const span = trace.getSpan(context.active());
  if (!span) return {};
  const spanContext = span.spanContext();
  return {
    traceId: spanContext.traceId,
    spanId: spanContext.spanId,
  };
}

// Wrap default logger to automatically include trace info
const baseLogger = {
  // Use a generic function for non-error levels
  log: (level: pino.Level, msg: string, meta?: any) => {
    logger[level]({ ...getTraceContext(), ...meta }, msg);
  },

  info: (msg: string, meta?: any) => baseLogger.log('info', msg, meta),
  warn: (msg: string, meta?: any) => baseLogger.log('warn', msg, meta),
  debug: (msg: string, meta?: any) => baseLogger.log('debug', msg, meta),
  error: (msg: string, meta?: any) => {
    const traceContext = getTraceContext();
    
    // Check if the 'meta' object is actually an Error instance
    if (meta instanceof Error) {
      // 1. Pass the Error object using the 'err' property
      // 2. Add the trace context explicitly
      logger.error({ 
        err: meta, 
        ...traceContext 
      }, msg);
      return;
    }
    
    // Fallback for non-Error meta objects
    logger.error({ ...traceContext, ...meta }, msg);
  },
};

export default baseLogger;