import { inject, injectable } from "inversify";
import { IExecutionService } from "./execution.service.interface";
import TYPES from "@/config/inversify/types";
import { KafkaManager } from "@/lib/kafka/kafkaManager";
import { KafkaConsumerGroups } from "@/lib/kafka/kafkaConsumerGroups";
import { KafkaTopics } from "@/lib/kafka/kafkaTopics";
import { ICustomCodeExecJobPayload, ICustomCodeResult, IRunCodeExecJobPayload, IRunCodeResult, ISubmissionExecJobPayload, ISubmissionResult } from "@/dtos/jobPayload.dto";
import { REDIS_PREFIX } from "@/config/redis/keyPrefix";
import { ICacheProvider } from "@/providers/ICacheProvider.interface";
import { config } from "@/config";
import { Runner } from "@/lib/dockersdk/ContainerPool";
import { ExecutionResult, IJudgeProps } from "@/types/judge.types";
import logger from "@/utils/pinoLogger";


/**
 * Class responsible executing code and publish to kafka.
 * * @class
 * @implements {IExecutionService}
 */
@injectable()
export class ExecutionService implements IExecutionService {

    #_kafkaManager : KafkaManager
    #_cacheProvider : ICacheProvider
    #_runner : Runner

    constructor(
        @inject(TYPES.KafkaManager)
        kafkaManager : KafkaManager,

        @inject(TYPES.ICacheProvider)
        cacheProvider : ICacheProvider,

        @inject(TYPES.Runner)
        runner : Runner
    ){
        this.#_kafkaManager = kafkaManager;
        this.#_cacheProvider = cacheProvider;
        this.#_runner = runner;
    }

    async submitCodeExec(): Promise<void> {
        const topic = KafkaTopics.SUBMISSION_JOBS;
        const group = KafkaConsumerGroups.CE_SUB_NORMAL_EXEC;
        logger.info(`[CONSUMER_SERVICE] Setting up consumer for topic: ${topic} with group: ${group}`);

        await this.#_kafkaManager.createConsumer(
            group,
            topic,
            async (data : ISubmissionExecJobPayload) => {
                const method = 'submitCodeExec (Job)';
                const submissionId = data.submissionId;
                const idempotencyKey = `${REDIS_PREFIX.KAFKA_IDEMPOTENCY_KEY_SUBMIT_RESULT}:${submissionId}`;
                logger.info(`[CONSUMER_SERVICE] ${method}: Received job`, { submissionId, language: data.language, userId: data.userId });

                try {
                    // Idempotency Check
                    const alreadyProcessed = await this.#_cacheProvider.get(idempotencyKey);
                    if(alreadyProcessed){
                        logger.warn(`[CONSUMER_SERVICE] ${method}: Idempotency key found. Skipping execution.`, { submissionId });
                        return;
                    }
                    logger.debug(`[CONSUMER_SERVICE] ${method}: Idempotency check passed. Starting judge process.`, { submissionId });
                    
                    const executionResult = await this.judge({
                        code : data.executableCode,
                        language : data.language,
                        testCases : data.testCases,
                        mode : 'submit'
                    });
                    
                    const jobPayload : ISubmissionResult = {
                        submissionId : submissionId,
                        userId : data.userId,
                        executionResult : executionResult,
                    }
                    
                    logger.debug(`[CONSUMER_SERVICE] ${method}: Judge complete. Status: ${executionResult.stats?.passedTestCase === executionResult.stats?.totalTestCase ? 'AC' : 'WA/ERR'}`);

                    await this.#_kafkaManager.sendMessage(
                        KafkaTopics.SUBMISSION_RESULTS,
                        jobPayload.submissionId,
                        jobPayload
                    );
                    logger.info(`[CONSUMER_SERVICE] ${method}: Result published to ${KafkaTopics.SUBMISSION_RESULTS}.`, { submissionId });

                    await this.#_cacheProvider.set(
                        idempotencyKey,
                        '1', 
                        config.KAFKA_IDEMPOTENCY_KEY_EXPIRY
                    ); 
                    logger.debug(`[CONSUMER_SERVICE] ${method}: Idempotency key set.`, { submissionId });

                } catch (error) {
                    logger.error(`[CONSUMER_SERVICE] ${method}: Error processing submission job. Throwing to trigger retry logic.`, { submissionId, error });
                    throw error;
                }
            }
        )
    }

    async runCodeExec(): Promise<void> {
        const topic = KafkaTopics.RUN_JOBS;
        const group = KafkaConsumerGroups.CE_RUN_NORMAL_EXEC;
        logger.info(`[CONSUMER_SERVICE] Setting up consumer for topic: ${topic} with group: ${group}`);

        await this.#_kafkaManager.createConsumer(
            group,
            topic,
            async (data : IRunCodeExecJobPayload) => {
                const method = 'runCodeExec (Job)';
                const tempId = data.tempId;
                const idempotencyKey = `${REDIS_PREFIX.KAFKA_IDEMPOTENCY_KEY_RUN_RESULT}:${tempId}`;
                logger.info(`[CONSUMER_SERVICE] ${method}: Received job`, { tempId, language: data.language });

                try {
                    // Idempotency Check
                    const alreadyProcessed = await this.#_cacheProvider.get(idempotencyKey);
                    if(alreadyProcessed){
                        logger.warn(`[CONSUMER_SERVICE] ${method}: Idempotency key found. Skipping execution.`, { tempId });
                        return;
                    }
                    logger.debug(`[CONSUMER_SERVICE] ${method}: Idempotency check passed. Starting judge process.`, { tempId });

                    const executionResult = await this.judge({
                        code : data.executableCode,
                        language : data.language,
                        testCases : data.testCases,
                        mode : 'run'
                    });
                    
                    const jobPayload : IRunCodeResult = {
                        tempId : tempId,
                        executionResult : executionResult                     
                    }

                    logger.debug(`[CONSUMER_SERVICE] ${method}: Judge complete. Status: ${executionResult.stats?.passedTestCase === executionResult.stats?.totalTestCase ? 'AC' : 'WA/ERR'}`);

                    await this.#_kafkaManager.sendMessage(
                        KafkaTopics.RUN_RESULTS,
                        jobPayload.tempId,
                        jobPayload
                    )
                    logger.info(`[CONSUMER_SERVICE] ${method}: Result published to ${KafkaTopics.RUN_RESULTS}.`, { tempId });

                    await this.#_cacheProvider.set(
                        idempotencyKey,
                        '1', 
                        config.KAFKA_IDEMPOTENCY_KEY_EXPIRY
                    );
                    logger.debug(`[CONSUMER_SERVICE] ${method}: Idempotency key set.`, { tempId });

                } catch (error) {
                    logger.error(`[CONSUMER_SERVICE] ${method}: Error processing run job. Throwing to trigger retry logic.`, { tempId, error });
                    throw error;
                }
            }
        )
    }

    async customCodeExec(): Promise<void> {
        const topic = KafkaTopics.CUSTOM_JOBS;
        const group = KafkaConsumerGroups.CE_CUSTOM_NORMAL_EXEC;
        logger.info(`[CONSUMER_SERVICE] Setting up consumer for topic: ${topic} with group: ${group}`);

        await this.#_kafkaManager.createConsumer(
            group,
            topic,
            async (data : ICustomCodeExecJobPayload) => {
                const method = 'customCodeExec (Job)';
                const tempId = data.tempId;
                const idempotencyKey = `${REDIS_PREFIX.KAFKA_IDEMPOTENCY_KEY_CUSTOM_RESULT}:${tempId}`;
                logger.info(`[CONSUMER_SERVICE] ${method}: Received job`, { tempId, language: data.language });

                try {
                    // Idempotency Check
                    const alreadyProcessed = await this.#_cacheProvider.get(idempotencyKey);
                    if(alreadyProcessed){
                        logger.warn(`[CONSUMER_SERVICE] ${method}: Idempotency key found. Skipping execution.`, { tempId });
                        return;
                    }
                    logger.debug(`[CONSUMER_SERVICE] ${method}: Idempotency check passed. Starting custom code execution.`, { tempId });

                    const codeToRun = JSON.parse(data.userCode);
                    const result = await this.#_runner.runCode(
                        data.language,
                        codeToRun,
                        true // isCustom
                    );
                    
                    const jobPayload : ICustomCodeResult = {
                        tempId : tempId,
                        stdOut : result.success ? result.stdout! : result.stderr!
                    }
                    logger.debug(`[CONSUMER_SERVICE] ${method}: Execution complete. Success: ${result.success}`);

                    await this.#_kafkaManager.sendMessage(
                        KafkaTopics.CUSTOM_RESULTS,
                        data.tempId,
                        jobPayload
                    );
                    logger.info(`[CONSUMER_SERVICE] ${method}: Result published to ${KafkaTopics.CUSTOM_RESULTS}.`, { tempId });

                    await this.#_cacheProvider.set(
                        idempotencyKey,
                        '1', 
                        config.KAFKA_IDEMPOTENCY_KEY_EXPIRY
                    );
                    logger.debug(`[CONSUMER_SERVICE] ${method}: Idempotency key set.`, { tempId });

                } catch (error) {
                    logger.error(`[CONSUMER_SERVICE] ${method}: Error processing custom job. Throwing to trigger retry logic.`, { tempId, error });
                    throw error;
                }
            }
        )
    }

    private async judge(props: IJudgeProps): Promise<ExecutionResult> {
        const method = 'judge';
        const totalTestCases = props.testCases.length;
        const mode = props.mode;
        logger.info(`[JUDGE] ${method} started`, { mode, language: props.language, totalTestCases });

        try {
            const startTime = Date.now();

            // 1. Execute the generated code once
            logger.debug(`[JUDGE] Running code in Docker container...`, { mode, language: props.language });
            const result =  await this.#_runner.runCode(
                props.language,
                props.code,
            );
            const executionDuration = Date.now() - startTime;
            logger.debug(`[JUDGE] Docker execution finished. Success: ${result.success}, Duration: ${executionDuration}ms`);


            // 3. Handle compilation or global runtime errors
            if (!result.success) {
                const errorOutput = result.stderr ?? result.error ?? "Execution Error";
                logger.warn(`[JUDGE] Code execution failed with error.`, { errorOutput: errorOutput.substring(0, 100) });
                return {
                    stats: {
                        totalTestCase: totalTestCases,
                        passedTestCase: 0,
                        failedTestCase: totalTestCases,
                        executionTimeMs : 0,
                        memoryMB : 0
                    },
                    failedTestCase: {
                        index: 0,
                        input: props.testCases[0].input,
                        output: errorOutput,
                        expectedOutput: props.testCases[0].output,
                    },
                };
            }
            
            // 4. Parse the output from stdout
            const outputLines = result.judgeOutput?.trim().split('\n') || [];
            let passedTestCases = 0;
            let executionTimeMs = 0 
            let memoryMB = 0; 

            const allResults: ExecutionResult["testResults"] = [];

            logger.debug(`[JUDGE] Parsing ${outputLines.length} judge output lines.`);

            for (const line of outputLines) {
                try {
                    const testResult = JSON.parse(line);
                    
                    // Update max resources used
                    executionTimeMs = Math.max(executionTimeMs, parseFloat(testResult.executionTimeMs));
                    memoryMB = Math.max(memoryMB, parseFloat(testResult.memoryMB));

                    const testCase = props.testCases[testResult.index];
                    const passed = testResult.status === "passed";

                    if (passed) {
                        passedTestCases++;
                    } 
                    
                    if(mode === 'run'){
                        allResults.push({
                            Id: testCase.Id,
                            index: testResult.index,
                            input: testCase.input,
                            output: testResult.actual ? JSON.stringify(testResult.actual) : testCase.output, // backend must send `actual` here
                            expectedOutput: testCase.output,
                            passed,
                            executionTimeMs: parseFloat(testResult.executionTimeMs),
                            memoryMB: parseFloat(testResult.memoryMB),
                        });
                    } else if (mode === 'submit' && !passed) {
                        // stop immediately on first failure for 'submit' mode
                        logger.warn(`[JUDGE] Test case ${testResult.index} failed. Aborting submission judge.`, { index: testResult.index });
                        return {
                            stats: {
                                totalTestCase: totalTestCases,
                                passedTestCase: passedTestCases,
                                failedTestCase: totalTestCases - passedTestCases,
                                executionTimeMs,
                                memoryMB,
                                stdout: result.stderr ?? undefined,
                            },
                            failedTestCase: {
                                index: testResult.index,
                                input: testCase.input,
                                output: testResult.actual ? JSON.stringify(testResult.actual) : testCase.output,
                                expectedOutput: testCase.output,
                            },
                        };
                    }
                } catch (e) {
                    // Handle cases where stdout is not valid JSON
                    logger.error(`[JUDGE] Invalid judge output received.`, { line, error: e });
                    return {
                        stats: {
                            totalTestCase : totalTestCases,
                            passedTestCase: 0,
                            failedTestCase: totalTestCases,
                            executionTimeMs,
                            memoryMB,
                        },
                        failedTestCase: {
                            index: 0,
                            input: "",
                            output: `Invalid judge output: ${line}`,
                            expectedOutput: "",
                        },
                    };
                }
            }

            // Return for 'run' mode (all results collected)
            if (mode === "run") {
                logger.info(`[JUDGE] Run mode judge completed. Passed: ${passedTestCases}/${totalTestCases}`);
                return {
                    stats: {
                        totalTestCase : totalTestCases,
                        passedTestCase : passedTestCases,
                        failedTestCase: totalTestCases - passedTestCases,
                        executionTimeMs,
                        memoryMB,
                        stdout: result.stdout ?? undefined,
                    },
                    testResults: allResults,
                };
            }


            // 5. All test cases passed for 'submit' mode
            logger.info(`[JUDGE] Submit mode judge completed. All ${totalTestCases} tests passed.`);
            return {
                stats: {
                    totalTestCase: totalTestCases,
                    passedTestCase: passedTestCases,
                    failedTestCase: 0,
                    executionTimeMs,
                    memoryMB,
                    stdout : result.stdout ?? undefined
                },
            };

        } catch (error) {
            logger.error(`[JUDGE] Critical internal error during code execution.`, { error });
            throw new Error(`Internal error while executing code, ${error}`);
        }
    }

}