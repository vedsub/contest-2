import express from "express"
import { podPool } from "../k8s/pod-pool"
import { Request, Response } from "express";
import { stepQueue } from "../queue/step-queue";
import { getReadySteps } from "./dag";
import { WorkflowState, StepState, StepStatus, QueuedStep } from "../types/workflow";

const app = express()
app.use(express.json())

// In-memory workflow store
const workflowStore: Map<string, WorkflowState> = new Map()

// POST /workflow
// Students implement: store workflow, find ready steps, enqueue them
app.post("/workflow", async (req, res) => {
  const workflow = req.body

  // TODO: students implement this in orchestrator.ts
  // Call orchestrator.submitWorkflow(workflow)
  void workflow
  res.status(501).json({ error: "Not implemented" })
})

// GET /workflow/:id
// Students implement: read from workflow-store and return current state
app.get("/workflow/:id", (req, res) => {
  const workflowId = req.params.id

  // TODO: students implement this
  void workflowId
  res.status(501).json({ error: "Not implemented" })
})

// GET /pods  — already implemented, useful for debugging
app.get("/pods", async (_req, res) => {
  const status = podPool.getPoolStatus()
  res.json(status)
})

export async function submitWorkflow(req: Request, res: Response) {
  const { workflowId, steps } = req.body;
  const stepState: Record<string, StepState> = {};

  for (const step of steps) {
    stepState[step.id] = {
      stepId: step.id,
      status: "PENDING",
      podId: null,
      retriesLeft: step.retries || 0
    };
  }

const workflowState: WorkflowState = {
    workflowId,
    status: "running",
    steps,
    stepState
  };
  workflowStore.set(workflowId, workflowState);

  // getReadySteps expects a map of stepId -> StepStatus (not the full StepState),
  // so build that from our stepState record.
  const stepStatusMap: Record<string, StepStatus> = {};
  for (const id of Object.keys(stepState)) {
    stepStatusMap[id] = stepState[id].status as StepStatus;
  }

  const readySteps = getReadySteps(steps, stepStatusMap);

  for (const step of readySteps) {
    workflowState.stepState[step.id].status = "QUEUED";
    
    const queuedStep: QueuedStep = {
      stepId: step.id,
      workflowId,
      command: step.command,
      enqueuedAt: Date.now()
    };
    
    await stepQueue.enqueue(queuedStep);
  }

  workflowStore.set(workflowId, workflowState);

  res.status(200).json({ workflowId, status: "accepted" });
}
