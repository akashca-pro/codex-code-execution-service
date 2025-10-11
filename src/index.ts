import logger from '@/utils/pinoLogger';
import container from '@/config/inversify/container';
import TYPES from '@/config/inversify/types';
import { KafkaManager } from '@/lib/kafka/kafkaManager';
import { KafkaTopics } from '@/lib/kafka/kafkaTopics';
import { ExecutionService } from '@/services/execution.service'

const startServer = async () => {
    logger.info('Starting Code Execution Service...');
    try {
        const kafkaManager = container.get<KafkaManager>(TYPES.KafkaManager);
        
        // kafka core setup
        logger.info('Initializing Kafka Manager...');
        await kafkaManager.init();
        logger.info('Kafka Manager initialized successfully.');

        // create topics
        logger.info('Creating necessary Kafka topics...');
        const topicsToCreate = [
            KafkaTopics.SUBMISSION_JOBS,
            KafkaTopics.SUBMISSION_RESULTS,
            KafkaTopics.RUN_JOBS,
            KafkaTopics.RUN_RESULTS,
            KafkaTopics.CUSTOM_JOBS,
            KafkaTopics.CUSTOM_RESULTS
        ];

        for (const topic of topicsToCreate) {
            await kafkaManager.createTopic(topic);
            logger.debug(`Kafka topic created/checked: ${topic}`);
        }
        logger.info('All necessary Kafka topics created/checked.');

        // start consumers
        logger.info('Starting Kafka Consumers via Execution Service...');
        const executionService = container.get<ExecutionService>(TYPES.IExecutionService);
        
        await executionService.submitCodeExec();
        logger.info(`Consumer started for topic: ${KafkaTopics.SUBMISSION_JOBS}`);
        
        await executionService.runCodeExec();
        logger.info(`Consumer started for topic: ${KafkaTopics.RUN_JOBS}`);
        
        await executionService.customCodeExec();
        logger.info(`Consumer started for topic: ${KafkaTopics.CUSTOM_JOBS}`);

        logger.info('All Kafka Consumers are running.');
        logger.info('Code Execution Service startup complete. Ready to process jobs.');

    } catch (error) {
        logger.error('Failed to start server : ',error);
        process.exit(1);
    }
}

startServer().catch((err)=>{
    logger.error(`Fatal startup error:`, err);
    process.exit(1);
});