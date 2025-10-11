import pino from 'pino'
import { context, trace } from '@opentelemetry/api'

const logger = pino({
  level: process.env.NODE_ENV === 'production' ? 'info' : 'debug',
  transport: process.env.NODE_ENV !== 'production'
    ? { target: 'pino-pretty', options: { colorize: true, translateTime: 'SYS:standard' } }
    : undefined,
})

// 👇 helper to extract trace & span IDs
function getTraceContext() {
  const span = trace.getSpan(context.active())
  if (!span) return {}
  const spanContext = span.spanContext()
  return {
    traceId: spanContext.traceId,
    spanId: spanContext.spanId,
  }
}

// Wrap default logger to automatically include trace info
const baseLogger = {
  info: (msg: string, meta?: any) => logger.info({ ...getTraceContext(), ...meta }, msg),
  error: (msg: string, meta?: any) => logger.error({ ...getTraceContext(), ...meta }, msg),
  warn: (msg: string, meta?: any) => logger.warn({ ...getTraceContext(), ...meta }, msg),
  debug: (msg: string, meta?: any) => logger.debug({ ...getTraceContext(), ...meta }, msg),
}

export default baseLogger
