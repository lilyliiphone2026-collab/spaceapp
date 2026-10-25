import { proxyActivities, sleep } from "@temporalio/workflow";

/**
 * Reference workflow for the replay gate: a fixed number of activities separated by
 * timers. Its recorded history must replay against this file unchanged.
 *
 * The sibling `drifted-workflows.ts` deliberately differs by one extra timer; the
 * replay test asserts that the drift is caught as a determinism violation.
 */
const { recordReplayProbeStep } = proxyActivities<{
  recordReplayProbeStep(input: { step: number }): Promise<void>;
}>({ startToCloseTimeout: "1 minute" });

export async function replayProbeWorkflow(): Promise<string> {
  await recordReplayProbeStep({ step: 1 });
  await sleep("1 minute");
  await recordReplayProbeStep({ step: 2 });
  await sleep("1 minute");
  await recordReplayProbeStep({ step: 3 });
  return "done";
}
