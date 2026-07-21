import { podPool } from "../k8s/pod-pool"
import { resultQueue } from "../queue/result-queue"
import type { QueuedStep } from "../types/workflow"

export class PodManager {
  async execute(step: QueuedStep): Promise<void> {
    let pod
    
    try {
      pod = await podPool.acquirePod()
      
      await resultQueue.push({
        stepId: step.stepId,
        workflowId: step.workflowId,
        podId: pod.podId,
        status: "RUNNING"
      })

      const stdout = await podPool.execInPod(pod.podId, step.command)

      await resultQueue.push({
        stepId: step.stepId,
        workflowId: step.workflowId,
        podId: pod.podId,
        status: "COMPLETED",
        stdout,
        exitCode: 0
      })

    } catch (error: any) {
      if (pod) {
        await resultQueue.push({
          stepId: step.stepId,
          workflowId: step.workflowId,
          podId: pod.podId,
          status: "FAILED",
          error: error.message || String(error),
          exitCode: 1
        })
      }
    } finally {
      if (pod) {
        await podPool.releasePod(pod.podId)
      }
    }
  }
}

export const podManager = new PodManager()