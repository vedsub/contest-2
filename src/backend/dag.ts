import type { WorkflowStep, StepStatus } from "../types/workflow"

/**
 * TODO: Implement this function.
 *
 * Given the full list of steps and the current status of each step,
 * return only the steps that are ready to run right now.
 *
 * A step is ready if:
 *   - All steps in its `dependsOn` array have status "COMPLETED"
 *   - Its own status is "PENDING" (not already QUEUED, RUNNING, etc.)
 *
 * Steps with no `dependsOn` are immediately ready (if PENDING).
 */
export function getReadySteps(
  steps: WorkflowStep[],
  stepStatus: Record<string, StepStatus>
): WorkflowStep[] {
  throw new Error("TODO: implement getReadySteps")
}
