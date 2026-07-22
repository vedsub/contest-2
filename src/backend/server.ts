import express, { Request, Response } from "express"
import { orchestrator } from "./orchestrator"
import { getWorkflow } from "./workflow-store"

export const app = express()
app.use(express.json())

app.post("/workflow", async (req: Request, res: Response) => {
  try {
    await orchestrator.submitWorkflow(req.body)
    res.status(200).json({ 
      workflowId: req.body.workflowId, 
      status: "accepted" 
    })
  } catch (error: any) {
    res.status(500).json({ error: error.message })
  }
})

app.get("/workflow/:id", (req: Request, res: Response) => {
  const id = Array.isArray(req.params.id) ? req.params.id[0] : req.params.id
  const wf = getWorkflow(id)

  if (!wf) {
    return res.status(404).json({ error: "Workflow not found" })
  }

  const steps = wf.steps.map(step => {
    const state = wf.stepState[step.id]
    return {
      id: step.id,
      status: state.status,
      podId: state.podId,
      stdout: state.stdout,
      exitCode: state.exitCode
    }
  })

  res.status(200).json({
    workflowId: wf.workflowId,
    status: wf.status,
    steps
  })
})