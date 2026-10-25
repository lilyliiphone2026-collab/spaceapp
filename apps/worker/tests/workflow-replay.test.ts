import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { Worker } from "@temporalio/worker";
import { temporal } from "@temporalio/proto";

const History = temporal.api.history.v1.History as any;

/**
 * Replay gate (CORE-006):
 *
 * Every history in `fixtures/workflow-histories/` was recorded by
 * `scripts/record-workflow-histories.mjs` against the workflow code at the time of
 * recording. Replaying them here fails whenever a workflow change is not
 * replay-compatible with running executions, so a determinism break shows up in CI
 * instead of in a user's installation.
 *
 * When a replay fails: either the change broke replay (fix the code, or wrap the
 * change in `patched()`), or the history is stale and must be re-recorded together
 * with a `patched()` change. Never re-record just to make this test pass.
 */

const FIXTURES = fileURLToPath(new URL("./fixtures/workflow-histories/", import.meta.url));

const BUNDLES: Record<string, string> = {
  main: fileURLToPath(new URL("../src/workflows.ts", import.meta.url)),
  maintenance: fileURLToPath(new URL("../src/memory-maintenance-workflows.ts", import.meta.url)),
  mutation: fileURLToPath(new URL("../src/memory-mutation-workflows.ts", import.meta.url))
};

type Recorded = { workflowId: string; history: { events: Array<Record<string, any>> } };

async function historiesFor(prefix: string): Promise<Recorded[]> {
  const files = (await readdir(FIXTURES)).filter((file) => file.endsWith(".json") && file.startsWith(`${prefix}-`)).sort();
  return Promise.all(files.map(async (file) => {
    const json = JSON.parse(await readFile(join(FIXTURES, file), "utf8"));
    // `Worker.runReplayHistories` converts a history through `historyFromJSON` whenever
    // events[0].eventId is a string. That helper resolves its protobufjs instance from
    // @temporalio/common while the type it looks up comes from @temporalio/proto, and
    // this repository's lockfile pins one protobufjs install per package, so the lookup
    // fails with "type must be a Type". `History.fromObject` from @temporalio/proto builds
    // the message with the same instance the worker encodes it with, so the ids arrive as
    // Longs and the worker hands the object straight to the replay bridge.
    const history = History.fromObject(json) as unknown as Recorded["history"];
    const started = (json.events as Array<Record<string, any>>).find((event) => event.workflowExecutionStartedEventAttributes);
    return {
      workflowId: started?.workflowExecutionStartedEventAttributes?.workflowId ?? file.replace(/\.json$/, ""),
      history
    };
  }));
}

for (const [prefix, workflowsPath] of Object.entries(BUNDLES)) {
  describe(`${prefix} workflow histories`, () => {
    it("replays every recorded history without a determinism violation", async () => {
      const recorded = await historiesFor(prefix);
      expect(recorded.length).toBeGreaterThan(0);

      const failures: Array<{ workflowId: string; error: string }> = [];
      for await (const result of Worker.runReplayHistories({ workflowsPath }, recorded)) {
        if (result.error) {
          failures.push({ workflowId: result.workflowId, error: `${result.error.name}: ${result.error.message}` });
        }
      }

      expect(failures).toEqual([]);
    }, 180_000);
  });
}
