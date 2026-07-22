import type { Workflow, WorkflowState, StepState, QueuedStep, StepStatus } from "../types/workflow"
import { getWorkflow, setWorkflow } from "./workflow-store"
import { getReadySteps } from "./dag"
import { stepQueue } from "../queue/step-queue"
import { resultQueue } from "../queue/result-queue"
import { podManager } from "../pod-manager/pod-manager"
import { podPool } from "../k8s/pod-pool"

export class Orchestrator {
  async submitWorkflow(workflow: Workflow): Promise<void> {
    const stepState: Record<string, StepState> = {}

    for (const step of workflow.steps) {
      stepState[step.id] = {
        stepId: step.id,
        status: "PENDING",
        podId: null,
        retriesLeft: step.retries || 0
      }
    }

    const workflowState: WorkflowState = {
      workflowId: workflow.workflowId,
      status: "running",
      steps: workflow.steps,
      stepState
    }

    setWorkflow(workflow.workflowId, workflowState)

    const stepStatusMap: Record<string, StepStatus> = {}
    for (const [id, state] of Object.entries(stepState)) {
      stepStatusMap[id] = state.status
    }

    const readySteps = getReadySteps(workflow.steps, stepStatusMap)

    for (const step of readySteps) {
      workflowState.stepState[step.id].status = "QUEUED"
      
      const queuedStep: QueuedStep = {
        stepId: step.id,
        workflowId: workflow.workflowId,
        command: step.command,
        enqueuedAt: Date.now()
      }
      
      await stepQueue.enqueue(queuedStep)
    }

    setWorkflow(workflow.workflowId, workflowState)
  }

  async start(): Promise<void> {
    resultQueue.consume(async (result) => {
      const wf = getWorkflow(result.workflowId)
      if (!wf) return

      const stepState = wf.stepState[result.stepId]
      stepState.status = result.status
      stepState.podId = result.podId
      stepState.stdout = result.stdout
      stepState.exitCode = result.exitCode
      stepState.error = result.error

      if (result.status === "FAILED") {
        let changed = true
        while (changed) {
          changed = false
          for (const step of wf.steps) {
            if (wf.stepState[step.id].status === "PENDING") {
              const hasFailedOrSkippedDep = step.dependsOn?.some(
                dep => wf.stepState[dep].status === "FAILED" || wf.stepState[dep].status === "SKIPPED"
              )
              if (hasFailedOrSkippedDep) {
                wf.stepState[step.id].status = "SKIPPED"
                changed = true
              }
            }
          }
        }
      }

      if (result.status === "COMPLETED") {
        const stepStatusMap: Record<string, StepStatus> = {}
        for (const [id, state] of Object.entries(wf.stepState)) {
          stepStatusMap[id] = state.status
        }

        const readySteps = getReadySteps(wf.steps, stepStatusMap)
        
        for (const step of readySteps) {
          wf.stepState[step.id].status = "QUEUED"
          
          const queuedStep: QueuedStep = {
            stepId: step.id,
            workflowId: wf.workflowId,
            command: step.command,
            enqueuedAt: Date.now()
          }
          
          await stepQueue.enqueue(queuedStep)
        }
      }

      const states = Object.values(wf.stepState)
      const allTerminal = states.every(s => s.status === "COMPLETED" || s.status === "SKIPPED" || s.status === "FAILED")
      const anyFailed = states.some(s => s.status === "FAILED")

      if (anyFailed) {
        wf.status = "failed"
      } else if (allTerminal) {
        wf.status = "completed"
      }

      setWorkflow(wf.workflowId, wf)
    })

    this.startStepDispatcher()
  }

  private async startStepDispatcher() {
    console.log("✅ Step dispatcher loop started!")
    while (true) {
      try {
        const poolStatus = podPool.getPoolStatus()
        
        if (poolStatus.available > 0) {
          const step = await stepQueue.dequeue()
          if (step) {
            console.log(`🚀 Picked up step ${step.stepId}. Sending to PodManager in background...`)
            
            podManager.execute(step).catch(error => {
              console.error(error)
            })
            
            continue
          }
        }
        
        await new Promise(resolve => setTimeout(resolve, 1000))
      } catch (error) {
        console.error(error)
        await new Promise(resolve => setTimeout(resolve, 1000))
      }
    }
  }
}

export const orchestrator = new Orchestrator()