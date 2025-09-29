import logger from '@/utils/logger';
import container from '@/config/inversify/container';
import TYPES from '@/config/inversify/types';
import { KafkaManager } from '@/lib/kafka/kafkaManager';
import { KafkaTopics } from '@/lib/kafka/kafkaTopics';
import { ExecutionService } from '@/services/execution.service'

const startServer = async () => {
    try {
        const kafkaManager = container.get<KafkaManager>(TYPES.KafkaManager);
        
        // kafka core setup
        await kafkaManager.init();

        // create topics
        await kafkaManager.createTopic(KafkaTopics.SUBMISSION_JOBS);
        await kafkaManager.createTopic(KafkaTopics.SUBMISSION_RESULTS);
        await kafkaManager.createTopic(KafkaTopics.RUN_JOBS);
        await kafkaManager.createTopic(KafkaTopics.RUN_RESULTS);
        await kafkaManager.createTopic(KafkaTopics.CUSTOM_JOBS);
        await kafkaManager.createTopic(KafkaTopics.CUSTOM_RESULTS);

        // start consumers
        const executionService = container.get<ExecutionService>(TYPES.IExecutionService);
        await executionService.submitCodeExec();
        await executionService.runCodeExec();
        await executionService.customCodeExec();

    } catch (error) {
        logger.error('Failed to start server : ',error);
        process.exit(1);
    }
}

startServer().catch((err)=>{
    logger.error(`Fatal startup error:`, err);
    process.exit(1);
});