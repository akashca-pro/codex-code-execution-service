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
import logger from "@/utils/logger";


/**
 * Class responsible executing code and publish to kafka.
 * 
 * @class
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
        await this.#_kafkaManager.createConsumer(
            KafkaConsumerGroups.CE_SUB_NORMAL_EXEC,
            KafkaTopics.SUBMISSION_JOBS,
            async (data : ISubmissionExecJobPayload) => {
                const idempotencyKey = `${REDIS_PREFIX.KAFKA_IDEMPOTENCY_KEY_SUBMIT_RESULT}:${data.submissionId}`;
                try {
                    const alreadyProcessed = await this.#_cacheProvider.get(idempotencyKey);
                    if(alreadyProcessed){
                        return;
                    }
                    const executionResult = await this.judge({
                        code : data.executableCode,
                        language : data.language,
                        testCases : data.testCases,
                        mode : 'submit'
                    });
                    const jobPayload : ISubmissionResult = {
                        submissionId : data.submissionId,
                        userId : data.userId,
                        executionResult : executionResult,
                    }
                    console.log(executionResult)
                    await this.#_kafkaManager.sendMessage(
                        KafkaTopics.SUBMISSION_RESULTS,
                        jobPayload.submissionId,
                        jobPayload
                    );
                    await this.#_cacheProvider.set(
                        idempotencyKey,
                        '1', 
                        config.KAFKA_IDEMPOTENCY_KEY_EXPIRY
                    ); 
                } catch (error) {
                    logger.error('Error processing submission job from Kafka:', error);
                    throw error;
                }
            }
        )
    }

    async runCodeExec(): Promise<void> {
        await this.#_kafkaManager.createConsumer(
            KafkaConsumerGroups.CE_RUN_NORMAL_EXEC,
            KafkaTopics.RUN_JOBS,
            async (data : IRunCodeExecJobPayload) => {
                const idempotencyKey = `${REDIS_PREFIX.KAFKA_IDEMPOTENCY_KEY_RUN_RESULT}:${data.tempId}`;
                try {
                    const alreadyProcessed = await this.#_cacheProvider.get(idempotencyKey);
                    if(alreadyProcessed){
                        return;
                    }
                    const executionResult = await this.judge({
                        code : data.executableCode,
                        language : data.language,
                        testCases : data.testCases,
                        mode : 'run'
                    });
                    const jobPayload : IRunCodeResult = {
                        tempId : data.tempId,
                        executionResult : executionResult                     
                    }
                    await this.#_kafkaManager.sendMessage(
                        KafkaTopics.RUN_RESULTS,
                        jobPayload.tempId,
                        jobPayload
                    )
                    await this.#_cacheProvider.set(
                        idempotencyKey,
                        '1', 
                        config.KAFKA_IDEMPOTENCY_KEY_EXPIRY
                    );
                } catch (error) {
                    logger.error('Error processing submission job from Kafka:', error);
                    throw error;
                }
            }
        )
    }

    async customCodeExec(): Promise<void> {
        await this.#_kafkaManager.createConsumer(
            KafkaConsumerGroups.CE_CUSTOM_NORMAL_EXEC,
            KafkaTopics.CUSTOM_JOBS,
            async (data : ICustomCodeExecJobPayload) => {
                const idempotencyKey = `${REDIS_PREFIX.KAFKA_IDEMPOTENCY_KEY_CUSTOM_RESULT}:${data.tempId}`;
                try {
                    const alreadyProcessed = await this.#_cacheProvider.get(idempotencyKey);
                    if(alreadyProcessed){
                        return;
                    }
                    const result = await this.#_runner.runCode(
                        data.language,
                        JSON.parse(data.userCode),
                        true
                    );
                    const jobPayload : ICustomCodeResult = {
                        tempId : data.tempId,
                        stdOut : result.success ? result.stdout! : result.stderr!
                    }
                    await this.#_kafkaManager.sendMessage(
                        KafkaTopics.CUSTOM_RESULTS,
                        data.tempId,
                        jobPayload
                    );
                    await this.#_cacheProvider.set(
                        idempotencyKey,
                        '1', 
                        config.KAFKA_IDEMPOTENCY_KEY_EXPIRY
                    );
                } catch (error) {
                    logger.error('Error processing submission job from Kafka:', error);
                    throw error;
                }
            }
        )
    }

    private async judge(props: IJudgeProps): Promise<ExecutionResult> {
        const totalTestCases = props.testCases.length;

        try {

            // 1. Execute the generated code once
            const result =  await this.#_runner.runCode(
                props.language,
                props.code,
            );

            // 3. Handle compilation or global runtime errors
            if (!result.success) {
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
                        output: result.stderr ?? result.error ?? "Execution Error",
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

            for (const line of outputLines) {
                try {
                    const testResult = JSON.parse(line);
                    executionTimeMs = Math.max(executionTimeMs, parseFloat(testResult.executionTimeMs));
                    memoryMB = Math.max(memoryMB, parseFloat(testResult.memoryMB));

                    const testCase = props.testCases[testResult.index];
                    const passed = testResult.status === "passed";

                    if(props.mode === 'run'){
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
                    }

                    if (passed) {
                        passedTestCases++;
                    } else if (props.mode === 'submit') {
                        // stop immediately on first failure
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

                if (props.mode === "run") {
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


            // 5. All test cases passed
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
            throw new Error(`Internal error while executing code, ${error}`);
        }
    }

}