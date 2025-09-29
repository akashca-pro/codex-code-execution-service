import 'reflect-metadata' 
import { Container, type ResolutionContext } from "inversify";
import TYPES from './types'
import { KafkaManager } from '@/lib/kafka/kafkaManager';
import { RedisCacheProvider } from '@/providers/RedisCacheProvider';
import { ICacheProvider } from '@/providers/ICacheProvider.interface';
import { IExecutionService } from '@/services/execution.service.interface';
import { ExecutionService } from '@/services/execution.service';
import { ContainerPool, Runner } from '@/lib/dockersdk/ContainerPool';
import { config } from '..';

const container = new Container();

container
    .bind<ICacheProvider>(TYPES.ICacheProvider)
    .to(RedisCacheProvider)
    .inSingletonScope();

// Bind ContainerPool as singleton
container.bind<ContainerPool>(TYPES.ContainerPool).toDynamicValue(() => {
  const pool = ContainerPool.getInstance({ image: config.WORKER_IMAGE_NAME });
  pool.init().catch(console.error); // async init
  pool.startMonitoring();
  return pool;
});

// Bind Runner as singleton, depending on pool
container.bind<Runner>(TYPES.Runner).toDynamicValue((context: ResolutionContext) => {
  const pool = context.get<ContainerPool>(TYPES.ContainerPool);
  return Runner.getInstance(pool);
}).inSingletonScope();


container
    .bind<KafkaManager>(TYPES.KafkaManager)
    .toConstantValue(KafkaManager.getInstance());

container
    .bind<IExecutionService>(TYPES.IExecutionService)
    .to(ExecutionService).inSingletonScope();

export default container