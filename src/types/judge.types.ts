import { Language } from "@/enums/Language.enum";


export interface IJudgeProps {
    code: string;
    language: Language;
    testCases: {
        Id: string;
        input: string;
        output: string;
    }[];
}

interface Stats {
    totalTestCase: number;
    passedTestCase: number;
    failedTestCase: number;
    stdout? : string;
    executionTimeMs?: number;
    memoryMB? : number;
}

interface FailedTestCase {
    index: number;
    input: string;
    output: any; // Can be string for error, or array for wrong answer
    expectedOutput: any;
}
export interface ExecutionResult {
    stats?: Stats | undefined;
    failedTestCase?: FailedTestCase | undefined;
}