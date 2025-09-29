import { Language } from "@/enums/Language.enum";
import { ExecutionResult } from "@/types/judge.types";

export interface TestCase {
    Id: string;
    input: string;
    output: string;
}

export interface ISubmissionExecJobPayload {
    submissionId : string;
    executableCode :string;
    language : Language;
    userId : string;
    testCases : {
        Id : string;
        input : string;
        output : string;
    }[];
}

export interface ISubmissionResult {
    submissionId : string;
    userId : string;
    executionResult : ExecutionResult
}

export interface IRunCodeExecJobPayload {
    tempId : string;
    language : Language;
    userCode : string;
    executableCode : string;
    testCases : TestCase[];
}

export interface IRunCodeResult {
    tempId : string;
    executionResult : ExecutionResult;
}

export interface ICustomCodeExecJobPayload {
    tempId : string;
    userCode : string;
    language : Language;
}

export interface ICustomCodeResult {
    tempId : string;
    stdOut : string;
}