import { NodeSDK } from '@opentelemetry/sdk-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-grpc';
import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-grpc';
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node';
import { PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics';
import { resourceFromAttributes } from '@opentelemetry/resources';
import { ATTR_SERVICE_NAME } from '@opentelemetry/semantic-conventions';
import { diag, DiagConsoleLogger, DiagLogLevel } from '@opentelemetry/api';
import * as grpc from '@grpc/grpc-js';
import { config } from '.';
import logger from '@/utils/pinoLogger';
import dotenv from 'dotenv';

dotenv.config();
diag.setLogger(new DiagConsoleLogger(), DiagLogLevel.INFO);

const serviceName = process.env.OTEL_SERVICE_NAME || config.SERVICE_NAME;
const otelCollectorEndpoint = process.env.OTEL_EXPORTER_OTLP_ENDPOINT;

let sdk: NodeSDK | undefined;

if (!otelCollectorEndpoint) {
  logger.info('OTEL_EXPORTER_OTLP_ENDPOINT not set, skipping OpenTelemetry initialization.');
} else {
  logger.info(`Initializing OpenTelemetry for service: ${serviceName}`);
  logger.info(`OTLP Collector endpoint: ${otelCollectorEndpoint}`);

  const resource = resourceFromAttributes({
    [ATTR_SERVICE_NAME]: serviceName,
  });

  const grpcCredentials = grpc.credentials.createInsecure();

  const traceExporter = new OTLPTraceExporter({
    url: otelCollectorEndpoint,
    credentials: grpcCredentials,
    timeoutMillis: 5000,
  });

  const metricExporter = new OTLPMetricExporter({
    url: otelCollectorEndpoint,
    credentials: grpcCredentials,
    timeoutMillis: 5000,
  });

  sdk = new NodeSDK({
    resource,
    traceExporter,
    metricReader: new PeriodicExportingMetricReader({
      exporter: metricExporter,
      exportIntervalMillis: 10000,
    }),
    instrumentations: getNodeAutoInstrumentations({
      '@opentelemetry/instrumentation-fs': {
        enabled: false,
      },
      '@opentelemetry/instrumentation-grpc': { 
        enabled: true 
      },
      '@opentelemetry/instrumentation-ioredis': {
        enabled: true
      },
      '@opentelemetry/instrumentation-redis': {
        enabled: true
      },
      '@opentelemetry/instrumentation-pino': {
        enabled: true
      },
      '@opentelemetry/instrumentation-kafkajs' : {enabled : true},
    }),
  });

  const shutdown = async () => {
    logger.info('Shutting down OpenTelemetry SDK...');
    try {
      await sdk?.shutdown();
      logger.info('OpenTelemetry SDK terminated successfully');
    } catch (error) {
      logger.error('Error terminating OpenTelemetry SDK:', error);
    }
  };

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);

  try {
    sdk.start();
    logger.info('OpenTelemetry SDK started successfully');
  } catch (error) {
    logger.error('Error starting OpenTelemetry SDK:', error);
  }
}

export default sdk;