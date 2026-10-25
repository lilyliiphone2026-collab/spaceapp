import { proxyActivities, sleep } from "@temporalio/workflow";

/**
 * The same workflow as `clean-workflows.ts` with one extra timer at the start.
 *
 * It exists only so the replay gate can prove it still catches drift: replaying a
 * history recorded by the clean file against this file must fail with
 * `DeterminismViolationError`. Nothing in production references this file.
 */
const { recordReplayProbeStep } = proxyActivities<{
  recordReplayProbeStep(input: { step: number }): Promise<void>;
}>({ startToCloseTimeout: "1 minute" });

export async function replayProbeWorkflow(): Promise<string> {
  await sleep("1 minute"); // <- the drift: a timer the recorded history never had
  await recordReplayProbeStep({ step: 1 });
  await sleep("1 minute");
  await recordReplayProbeStep({ step: 2 });
  await sleep("1 minute");
  await recordReplayProbeStep({ step: 3 });
  return "done";
}
