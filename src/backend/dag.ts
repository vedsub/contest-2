import type { WorkflowStep, StepStatus } from "../types/workflow"

export function getReadySteps(
  steps: WorkflowStep[],
  stepStatus: Record<string, StepStatus>
): WorkflowStep[] {
  return steps.filter(step => {
    const status = stepStatus[step.id];
    if (status === "QUEUED" || status === "RUNNING" || status === "COMPLETED") {
      return false;
    }
    if (!step.dependsOn || step.dependsOn.length === 0) {
      return true;
    }
    return step.dependsOn.every(dep => stepStatus[dep] === "COMPLETED");
  });
}
