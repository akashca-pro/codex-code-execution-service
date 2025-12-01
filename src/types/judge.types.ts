import { Language } from "@/enums/Language.enum";


export interface IJudgeProps {
    code: string;
    language: Language;
    testCases: {
        Id: string;
        input: string;
        output: string;
    }[];
    mode: "run" | "submit"
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

interface TestResult {
    Id?: string;
    index : string;
    input : string;
    output : any;
    expectedOutput: string;
    passed: boolean
    executionTimeMs: number
    memoryMB: number
}

export interface ExecutionResult {
    stats?: Stats | undefined;
    failedTestCase?: FailedTestCase | undefined;
    testResults? : TestResult[]
}