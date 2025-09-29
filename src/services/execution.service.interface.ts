
export interface IExecutionService {

    /**
     * Consumes submit code execution job from code manage service via kafka
     * executes the code produce submit code result job to code manage service via kafka.
     *  
     */
    submitCodeExec() : Promise<void>

    /**
     * Consumes run code (limited testcases) execution job from code manage service via kafka
     * executes the code produce run code result job to code manage service via kafka.
     * 
     */
    runCodeExec() : Promise<void>

    /**
     * Consumes the custom code execution job from code manage service via kafka
     * executes the code produce custom code result job to code manage service via kafka.
     * 
     */
    customCodeExec() : Promise<void>
}