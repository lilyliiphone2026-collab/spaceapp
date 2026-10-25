import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, expect, it } from "vitest";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { Worker } from "@temporalio/worker";
/**
 * Proves the replay gate itself works.
 *
 * A history recorded by `fixtures/replay/clean-workflows.ts` must replay without an
 * error against that same file, and must fail with `DeterminismViolationError`
 * against `fixtures/replay/drifted-workflows.ts`, which adds one timer. Without this
 * negative case a green replay run would prove nothing.
 */

const CLEAN_BUNDLE = fileURLToPath(new URL("./fixtures/replay/clean-workflows.ts", import.meta.url));
const DRIFTED_BUNDLE = fileURLToPath(new URL("./fixtures/replay/drifted-workflows.ts", import.meta.url));
const PROBE_TASK_QUEUE = "workflow-replay-probe";
const PROBE_WORKFLOW_ID = "workflow-replay-probe-clean";

type RecordedHistory = { workflowId: string; history: { events: unknown[] } };

let env: TestWorkflowEnvironment;
let recorded: RecordedHistory[] = [];

beforeAll(async () => {
  env = await TestWorkflowEnvironment.createTimeSkipping();
  const worker = await Worker.create({
    connection: env.nativeConnection,
    taskQueue: PROBE_TASK_QUEUE,
    workflowsPath: CLEAN_BUNDLE,
    activities: { recordReplayProbeStep: async () => undefined }
  });
  await worker.runUntil(async () => {
    const handle = await env.client.workflow.start("replayProbeWorkflow", {
      taskQueue: PROBE_TASK_QUEUE, workflowId: PROBE_WORKFLOW_ID, args: []
    });
    await expect(handle.result()).resolves.toBe("done");
    const description = await handle.describe();
    const response = await env.client.workflowService.getWorkflowExecutionHistory({
      namespace: "default",
      execution: { workflowId: PROBE_WORKFLOW_ID, runId: description.runId }
    });
    expect(response.history?.events?.length).toBeGreaterThan(0);
    recorded = [{ workflowId: PROBE_WORKFLOW_ID, history: response.history as { events: unknown[] } }];
  });
}, 300_000);

afterAll(async () => {
  await env?.teardown();
});

async function replay(workflowsPath: string) {
  const results = [];
  for await (const result of Worker.runReplayHistories({ workflowsPath }, recorded)) results.push(result);
  return results;
}

it("replays a recorded history against unchanged workflow code", async () => {
  const results = await replay(CLEAN_BUNDLE);
  expect(results).toHaveLength(1);
  expect(results[0]!.error).toBeUndefined();
}, 180_000);

it("fails replay with a determinism violation when a timer was added", async () => {
  const results = await replay(DRIFTED_BUNDLE);
  expect(results).toHaveLength(1);
  const error = results[0]!.error;
  // Compared by name: the class is defined in @temporalio/workflow, and importing it
  // here would load a second copy that `instanceof` cannot match.
  expect(error).toBeDefined();
  expect(error!.name).toBe("DeterminismViolationError");
  expect(String(error)).toContain("Replay");
}, 180_000);
