import fs from "fs";
import os from "os";

export function getCpuLimit(): number {
  try {
    // cgroup v2: /sys/fs/cgroup/cpu.max
    const data = fs.readFileSync("/sys/fs/cgroup/cpu.max", "utf8").trim();
    const [quotaStr, periodStr] = data.split(" ");
    if (quotaStr !== "max") {
      const quota = parseInt(quotaStr, 10);
      const period = parseInt(periodStr, 10);
      if (quota > 0 && period > 0) {
        return Math.ceil(quota / period);
      }
    }
  } catch {
    // ignore and fallback
  }

  return os.cpus().length; // fallback to visible CPUs
}