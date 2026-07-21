import type { QueuedStep } from "../types/workflow"
import { podPool } from "../k8s/pod-pool"
import { resultQueue } from "../queue/result-queue"

/**
 * TODO: Implement this class.
 *
 * The pod manager receives a step, runs it inside a leased pod,
 * and publishes status events to the result queue.
 *
 * dispatch(step) must:
 *   1. Call podPool.acquirePod() to lease a free pod
 *   2. Push { status: "RUNNING", podId, stepId, workflowId } to resultQueue
 *   3. Call podPool.execInPod(podId, step.command)
 *   4. Push { status: "COMPLETED", stdout, exitCode: 0 } to resultQueue
 *   5. Call podPool.releasePod(podId)
 *
 *   On any error:
 *   4. Push { status: "FAILED", error: err.message } to resultQueue
 *   5. Call podPool.releasePod(podId)  ← always release in finally block
 *
 * IMPORTANT: Pod manager never reads or writes workflow state.
 * It only pushes to the result queue.
 */
export class PodManager {
  async dispatch(step: QueuedStep): Promise<void> {
    void step
    void podPool
    void resultQueue
    throw new Error("TODO: implement dispatch")
  }
}

export const podManager = new PodManager()
export async function executeStep(step: QueuedStep): Promise<void> {
  let pod;
  
  try {
    pod = await podPool.acquirePod();
    
    await resultQueue.push({
      stepId: step.stepId,
      workflowId: step.workflowId,
      podId: pod.podId,
      status: "RUNNING"
    });

    const stdout = await podPool.execInPod(pod.podId, step.command);

    await resultQueue.push({
      stepId: step.stepId,
      workflowId: step.workflowId,
      podId: pod.podId,
      status: "COMPLETED",
      stdout,
      exitCode: 0
    });

  } catch (error: any) {
    if (pod) {
      await resultQueue.push({
        stepId: step.stepId,
        workflowId: step.workflowId,
        podId: pod.podId,
        status: "FAILED",
        error: error.message || String(error),
        exitCode: 1
      });
    }
  } finally {
    if (pod) {
      await podPool.releasePod(pod.podId);
    }
  }
}
