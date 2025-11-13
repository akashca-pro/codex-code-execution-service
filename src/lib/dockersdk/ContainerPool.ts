import Docker, { Container, ContainerCreateOptions } from "dockerode";
import fs from "fs";
import tar from "tar-stream";
import { PassThrough } from "stream";
import { config } from "@/config";
import { Language } from "@/enums/Language.enum";
import logger from "@/utils/pinoLogger";

export const docker = fs.existsSync(config.DOCKER_LOCAL)
  ? new Docker({ socketPath: config.DOCKER_LOCAL }) // Local dev
  : new Docker({ host: config.DOCKER_HOST, port : config.DOCKER_PORT }); // K8s DinD

interface ExecResult {
  success: boolean;
  stdout?: string;
  stderr?: string;
  error?: string;
  judgeOutput?: string; // This will hold the content of the results file
}

const MAX_OUTPUT_LENGTH = config.MAX_OUTPUT_LENGTH

export class ContainerPool {
  #_image: string;
  #_size: number;
  private pool: Container[] = [];
  private busy = new Set<Container>();
  private waiters: ((c: Container) => void)[] = [];
  private monitorInterval?: NodeJS.Timeout;
  private static _instance: ContainerPool;
  
  private constructor(props: { image: string; size?: number }) {
    this.#_image = props.image;
    this.#_size = props.size ? props.size : config.CONTAINER_POOL_SIZE
  }

  public static getInstance(props?: { image: string; size?: number }): ContainerPool {
    if (!ContainerPool._instance) {
      if (!props) throw new Error("ContainerPool not initialized yet. Pass props first time.");
      ContainerPool._instance = new ContainerPool(props);
    }
    return ContainerPool._instance;
  }

  // Centralized container configuration
  private getContainerOptions(): ContainerCreateOptions {
    return {
      Image: this.#_image,
      Tty: false,
      HostConfig: {
        NetworkMode: "none",  
        SecurityOpt: ["no-new-privileges"],
      },
      Cmd: ["tail", "-f", "/dev/null"],
    };
  }
  
  private async _createContainer(): Promise<Container> {
    logger.info("[POOL] Creating new container...");
    const container = await docker.createContainer(this.getContainerOptions());
    await container.start();
    // OPTIONAL BACKUP FIX: Ensure permissions are correct after tmpfs mount (runs as root)
    // await this._ensurePermissions(container);
    logger.info(`[POOL] New container ${container.id.substring(0, 12)} created and started.`);
    return container;
  }

async init() {
    logger.info("[POOL] Initializing container pool...");
    const allContainers = await docker.listContainers({ all: true });
    const existing = allContainers
      .filter(c => c.Image === this.#_image)
      .map(c => docker.getContainer(c.Id));

    this.pool = [...existing];
    logger.info(`[POOL] Found ${existing.length} existing containers.`);

    const toCreate = this.#_size - this.pool.length;
    for (let i = 0; i < toCreate; i++) {
      const newContainer = await this._createContainer();
      this.pool.push(newContainer);
    }

    logger.info(`${this.pool.length} containers ready (reused + new).`);
  }

  startMonitoring(intervalMs = 3000) {
    logger.info(`Starting container monitoring with ${intervalMs}ms interval.`);
    this.monitorInterval = setInterval(async () => {
      for (let i = 0; i < this.pool.length; i++) {
        const container = this.pool[i];
        // If the container is currently checked out and in use,
        // skip the health check for this cycle.
        if (this.busy.has(container)) {
          continue; 
        }
        try {
          const inspect = await container.inspect();
          if (!inspect.State.Running) {
            logger.warn(`Container ${container.id.substring(0, 12)} found dead. Replacing...`);
            await this.replaceContainer(i);
          }
        } catch (err) {
            // Container likely doesn't exist anymore
            logger.error(`Health check failed for container index ${i}. Replacing...`);
            await this.replaceContainer(i);
        }
      }
    }, intervalMs);
  }

  private async replaceContainer(index: number) {
      const oldContainer = this.pool[index];
      // Attempt to clean up the old container, but don't fail if it's already gone
      try {
        await oldContainer.remove({ force: true });
      } catch {}

      const newContainer = await this._createContainer();
      this.pool[index] = newContainer;
      logger.info(`Container at index ${index} replaced with ${newContainer.id.substring(0,12)}.`);
  }

  stopMonitoring() {
    if (this.monitorInterval) {
      clearInterval(this.monitorInterval);
      this.monitorInterval = undefined;
      logger.info("Container monitoring stopped.");
    }
  }

  async acquire(timeoutMs = 10000): Promise<Container> {
    // Find a container that is in the pool but not in the busy set
    const free = this.pool.find((c) => !this.busy.has(c));
    if (free) {
      this.busy.add(free);
      return free;
    }

    // If no free container, wait for one
    return new Promise<Container>((resolve, reject) => {
      const timer = setTimeout(() => {
        // Clean up the waiter function to prevent memory leaks
        this.waiters = this.waiters.filter(w => w !== waiter);
        reject(new Error("AcquireTimeout: No container available in time."));
      }, timeoutMs);

      const waiter = (container: Container) => {
        clearTimeout(timer);
        this.busy.add(container);
        resolve(container);
      };

      this.waiters.push(waiter);
    });
  }

  release(container: Container) {
    this.busy.delete(container);
    // If there are waiters, give the container to the next one in the queue
    const nextWaiter = this.waiters.shift();
    if (nextWaiter) {
      nextWaiter(container);
    }
  }

  async cleanup() {
    this.stopMonitoring();
    const cleanupPromises = this.pool.map(c => 
      c.remove({ force: true }).catch(err => {
        // Ignore errors if container is already gone
        logger.warn(`Could not remove container ${c.id.substring(0,12)}: ${err.message}`);
      })
    );
    await Promise.all(cleanupPromises);
    this.pool = [];
    this.busy.clear();
    this.waiters = [];
    logger.info("Container pool cleaned up.");
  }

}

export class Runner {

  private static _instance: Runner;

  private constructor(private pool: ContainerPool) {}

  public static getInstance(pool?: ContainerPool): Runner {
    if (!Runner._instance) {
      if (!pool) throw new Error("Runner not initialized yet. Pass ContainerPool first time.");
      Runner._instance = new Runner(pool);
    }
    return Runner._instance;
  }

  private async execInContainer(
    container: Container,
    cmd: string[],
    timeoutMs: number,
    stdinInput?: string
  ): Promise<ExecResult> {
    try {
      const exec = await container.exec({
        Cmd: cmd,
        AttachStdout: true,
        AttachStderr: true,
        AttachStdin: !!stdinInput,
      });

      const stream = await exec.start({ hijack: true, stdin: !!stdinInput });

      if (stdinInput) {
        stream.write(stdinInput);
        stream.end();
      }

      const stdoutStream = new PassThrough();
      const stderrStream = new PassThrough();
      docker.modem.demuxStream(stream, stdoutStream, stderrStream);

      let stdout = "";
      let stderr = "";

      stdoutStream.on("data", (chunk) => {
        if (stdout.length < MAX_OUTPUT_LENGTH) {
          stdout += chunk.toString();
          if (stdout.length > MAX_OUTPUT_LENGTH) {
            stdout = stdout.slice(0, MAX_OUTPUT_LENGTH) + "\n...output truncated...";
          }
        }
      });

      stderrStream.on("data", (chunk) => {
        if (stderr.length < MAX_OUTPUT_LENGTH) {
          stderr += chunk.toString();
          if (stderr.length > MAX_OUTPUT_LENGTH) {
            stderr = stderr.slice(0, MAX_OUTPUT_LENGTH) + "\n...error truncated...";
          }
        }
      });

      const timeout = new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error("TimeoutError: Execution took too long")), timeoutMs)
      );

      await Promise.race([
        new Promise<void>((resolve, reject) => {
          stream.on("end", resolve);
          stream.on("error", reject);
        }),
        timeout,
      ]);

      const inspect = await exec.inspect();
      if (inspect.ExitCode !== 0) {
        return { success: false, stderr: stderr || stdout };
      }

      stream.on("end", () => {
        stream.destroy();
      });

      return stderr
        ? { success: false, stderr }
        : { success: true, stdout };
    } catch (err) {
      return { success: false, error: (err as Error).message };
    }
  }

    async runCode(
      language: Language,
      code: string,
      customCode : boolean = false,
      timeoutMs = config.EXECUTION_TIMEOUT,
    ): Promise<ExecResult> {
      logger.info(`[RUNNER] 1. Attempting to acquire a container for language: ${language}.`);
      const container = await this.pool.acquire();
      logger.info(`[RUNNER] 2. Container acquired: ${container.id.substring(0, 12)}.`);
      
      // Define our standard paths within the sandbox
      const codeDir = '/sandbox/code';
      const binDir = '/sandbox/bin';
      const resultsDir = '/sandbox/results';
      const resultsPath = `${resultsDir}/output.json`; // The file we will read

      try {
        let execResult: ExecResult;

        if (language === "javascript") {
          const filename = `${codeDir}/code.js`;
          logger.info(`[RUNNER] 3a. Writing JavaScript code to ${filename}.`);
          await this.writeFile(container, filename, code);
          logger.info(`[RUNNER] 4a. Executing command: node ${filename}.`);
          // NOTE: The Node execution will run as 'appuser'
          execResult = await this.execInContainer(container, ["node", filename], timeoutMs);
        } else if (language === "python") {
          const filename = `${codeDir}/code.py`;
          logger.info(`[RUNNER] 3b. Writing Python code to ${filename}.`);
          await this.writeFile(container, filename, code);
          logger.info(`[RUNNER] 4b. Executing command: python3 ${filename}.`);
          // NOTE: The Python execution will run as 'appuser'
          execResult = await this.execInContainer(container, ["python3", filename], timeoutMs);
        } else if (language === "go") {
          const sourcePath = `${codeDir}/main.go`;
          const outputPath = `${binDir}/executable`;
          logger.info(`[RUNNER] 3c. Writing Go code to ${sourcePath}.`);
          await this.writeFile(container, sourcePath, code);

          logger.info(`[RUNNER] 4c. Compiling Go code: go build -o ${outputPath} ${sourcePath}.`);
          // NOTE: Go compilation command runs as 'appuser'
          const compileResult = await this.execInContainer(container, ["go", "build", "-o", outputPath, sourcePath], timeoutMs);
          
          if (!compileResult.success) {
            logger.info("[RUNNER] 5c. Go compilation failed. Returning error.");
            return compileResult;
          }
          logger.info("[RUNNER] 5c. Go compilation successful.");

          logger.info(`[RUNNER] 6c. Executing compiled Go binary: ${outputPath}.`);
          // NOTE: Execution of the binary runs as 'appuser'
          execResult = await this.execInContainer(container, [outputPath], timeoutMs);
        } else {
          logger.warn(`[RUNNER] 3d. Unsupported language: ${language}.`);
          return { success: false, error: `Unsupported language: ${language}` };
        }

        logger.info(`[RUNNER] 7. Code execution result: success=${execResult.success}.`);

        // If code execution was successful, try to read the judge output file
        if (execResult.success && !customCode) {
            logger.info(`[RUNNER] 8. Attempting to read judge output file: ${resultsPath}.`);
            try {
                const archiveStream = await container.getArchive({ path: resultsPath });
                const fileContent = await this.extractFileFromStream(archiveStream);
                execResult.judgeOutput = fileContent;
                logger.info("[RUNNER] 9. Judge output successfully read.");
            } catch (readError) {
                // File might not exist if the code crashed before writing it
                const errorMessage = `Failed to read results file: ${(readError as Error).message}`;
                execResult.stderr = errorMessage;
                execResult.success = false;
                logger.error(`[RUNNER] 9. Error reading judge output: ${errorMessage}`);
            }
        } else if(customCode){
            logger.info("[RUNNER] 8. Custom code execution success");
        }else{
            logger.info("[RUNNER] 8. Execution failed (stderr/timeout). Skipping judge output read.");
        }
        
        return execResult;

      } finally {
        logger.info("[RUNNER] 10. Starting cleanup.");
        
        const cleanupExec = await container.exec({
            Cmd: ["/bin/sh", "-c", `rm -rf ${codeDir}/* ${resultsDir}/* ${binDir}/*`],
            User: "root" 
        });

        // Get the stream from the cleanup command
        const stream = await cleanupExec.start({ hijack: true, stdin: false });
        
        // Wait for the stream to end. This ensures the command has finished executing.
        await new Promise<void>((resolve, reject) => {
            stream.on('end', resolve);
            stream.on('error', reject);
        });

        // Optional but recommended: Check the exit code to be certain cleanup succeeded
        const inspectResult = await cleanupExec.inspect();
        if (inspectResult.ExitCode !== 0) {
            logger.error(`[RUNNER] Cleanup failed with exit code: ${inspectResult.ExitCode}`);
        }
        logger.info("[RUNNER] 11. Cleanup complete. Releasing container.");
        this.pool.release(container);

      }
    }


    async writeFile(container: Container, fullPath: string, content: string): Promise<void> {
      const pack = tar.pack();

      // Extract directory and filename
      const parts = fullPath.split('/');
      const filename = parts.pop()!;
      const dir = parts.join('/') || '/';

        const mkdirExec = await container.exec({
            Cmd: ["mkdir", "-p", dir],
            User: "root",
        });
        await mkdirExec.start({});

      // Add file to tar archive with just the filename
        pack.entry({ name: filename }, content);
        pack.finalize();

      // Put archive into the correct directory
      await container.putArchive(pack, { path: dir });
    }

    async extractFileFromStream(stream: NodeJS.ReadableStream): Promise<string> {
        return new Promise((resolve, reject) => {
            const extract = tar.extract();
            let fileContent = "";

            extract.on('entry', (header, entryStream, next) => {
                const chunks: Buffer[] = [];
                entryStream.on('data', (chunk) => chunks.push(chunk));
                entryStream.on('end', () => {
                    fileContent = Buffer.concat(chunks).toString('utf8');
                    next();
                });
                entryStream.resume(); // Gulp requirement
            });

            extract.on('finish', () => resolve(fileContent));
            extract.on('error', reject);

            stream.pipe(extract);
        });
    }

  getPool() {
    return this.pool;
  }
}