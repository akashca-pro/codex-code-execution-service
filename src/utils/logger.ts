import winston from 'winston';
import dotenv from 'dotenv'
dotenv.config();

const customFormat = winston.format.printf(({ level, message, timestamp, service, stack }) => {
  if (stack) {
    return `[${timestamp}] [${level}] [${service}]: ${message}\n${stack}`;
  }
  return `[${timestamp}] [${level}] [${service}]: ${message}`;
});

const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  defaultMeta: { service: process.env.SERVICE_NAME },
  format: winston.format.combine(
    winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    winston.format.errors({ stack: true }), // Capture stack traces
    winston.format.splat(), //  For string interpolation
    winston.format.json() // Output a JSON object
  ),
  transports: [
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        customFormat 
      ),
    }),
  ],
});

export default logger;