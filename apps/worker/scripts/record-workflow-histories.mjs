#!/usr/bin/env node
/**
 * Records real Temporal histories for every workflow in apps/worker into
 * tests/fixtures/workflow-histories/ so `workflow-replay.test.ts` can replay them
 * against the current workflow code in CI.
 *
 * Run with `npm run temporal:record-histories -w @space/worker`.
 *
 * Re-record only together with a `patched()` change. Never re-record to make a
 * failing replay pass: a failing replay means a running execution would break.
 *
 * The scenarios here are intentionally minimal — one canonical run per workflow,
 * plus the supervisor's continue-as-new handover. They exist to produce a replayable
 * history, not to assert behaviour; behaviour lives in workflow-sandbox.test.ts.
 */
import { mkdir, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { Worker } from "@temporalio/worker";

const here = dirname(fileURLToPath(import.meta.url));
const fixtures = join(here, "..", "tests", "fixtures", "workflow-histories");

const MAIN = join(here, "..", "src", "workflows.ts");
const MAINTENANCE = join(here, "..", "src", "memory-maintenance-workflows.ts");
const MUTATION = join(here, "..", "src", "memory-mutation-workflows.ts");

const DEFAULT_TASK_QUEUE = "space-agent-turns";
const MEMORY_MAINTENANCE_TASK_QUEUE = "space-memory-maintenance";
const MEMORY_MUTATION_TASK_QUEUE = "space-memory-mutations";

const verifiedTurn = () => ({
  status: "COMPLETED",
  message: "ok",
  roomAgentOutcome: { status: "VERIFIED", executedActionCount: 0, statusReason: "Verified." }
});

const activities = {
  recordDummyTurnStarted: async () => undefined,
  recordDummyTurnCompleted: async () => ({ status: "COMPLETED", message: "ok" }),
  runCodexAppServerTurn: async () => verifiedTurn(),
  runOpenCodeAgentTurn: async () => verifiedTurn(),
  runCliAgentTurn: async () => verifiedTurn(),
  runRoomAgentBackgroundAction: async () => ({ status: "COMPLETED" }),
  markRoomAgentMissionStarted: async () => undefined,
  markRoomAgentMissionFinished: async () => undefined,
  markRoomAgentMissionContinued: async () => undefined,
  settleRoomAgentBackgroundMission: async () => true,
  checkRoomAgentSupervisorTurn: async () => "PROCEED",
  discardRoomAgentSupervisorTurn: async () => undefined,
  finishRoomAgentSupervisorTurn: async () => undefined,
  prepareRoomAgentBackgroundContinuation: async () => ({ state: "DONE" }),
  failRoomAgentBackgroundAction: async () => undefined,
  dotGoalStep: async () => ({ state: "DONE", runId: null, goalStatus: "COMPLETE" }),
  dotGoalObserve: async () => ({ run: "SUCCEEDED", goalStatus: "COMPLETE" }),
  refreshMemoryGraphSnapshot: async (input) => ({
    status: "REFRESHED", generatedAt: input.scheduledAt, sourceHash: "hash-1",
    previousSourceHash: null, summary: {}, durationMs: 1
  }),
  refreshAllMonthsMemoryGraphSnapshot: async () => undefined,
  reconcileStaleSpaceAgentSessions: async () => undefined,
  createScheduledMemoryAudit: async () => ({ id: "run-recorded", workflowId: "space-memory-maintenance:recorded" }),
  prepareMemoryConsolidation: async () => ({ changeSetId: "cs-recorded", run: { id: "run-recorded", status: "PENDING" } }),
  finalizeMemoryRepair: async (input) => ({ id: input.runId, status: "REPAIRED" }),
  executePersistedMemoryChangeSet: async () => ({ status: "APPLIED" })
};

const turn = { roomId: "room-recorder", paneId: "pane-recorder", prompt: "Record a history", artifactIds: [] };

const scenario = [
  { name: "main-dummy-turn", bundle: MAIN, queue: DEFAULT_TASK_QUEUE, type: "dummyTurnWorkflow", args: [turn] },
  { name: "main-codex-turn", bundle: MAIN, queue: DEFAULT_TASK_QUEUE, type: "codexAppServerTurnWorkflow", args: [turn] },
  {
    name: "main-room-action", bundle: MAIN, queue: DEFAULT_TASK_QUEUE, type: "roomAgentActionWorkflow",
    workflowId: "space-room-action:recorded", args: [{ bridge: { roomId: "room-recorder", missionId: "mission-recorder" } }]
  },
  { name: "main-dot-goal", bundle: MAIN, queue: DEFAULT_TASK_QUEUE, type: "dotGoalWorkflow", args: [{ goalId: "goal-recorder" }] },
  {
    name: "main-supervisor", bundle: MAIN, queue: DEFAULT_TASK_QUEUE, type: "roomAgentSupervisorWorkflow",
    args: [{
      roomId: "room-recorder", monitoringMissionIds: [], interrupted: [], processedCount: 49,
      pending: [{ missionId: "mission-recorder", turn: { ...turn, agentRunId: "agent-run-recorder", traceId: "trace-recorder-1" } }]
    }],
    terminateAfterMs: 1500
  },
  {
    name: "maintenance-run", bundle: MAINTENANCE, queue: MEMORY_MAINTENANCE_TASK_QUEUE, type: "memoryMaintenanceWorkflow",
    args: [{ scheduledAt: "2026-10-25T00:00:00.000Z", traceId: "trace-recorder" }]
  },
  {
    name: "maintenance-consolidation", bundle: MAINTENANCE, queue: MEMORY_MAINTENANCE_TASK_QUEUE, type: "memoryConsolidationWorkflow",
    args: [{ runId: "run-recorder", traceId: "trace-recorder" }]
  },
  {
    name: "mutation-run", bundle: MUTATION, queue: MEMORY_MUTATION_TASK_QUEUE, type: "memoryMutationWorkflow",
    args: [{ changeSetId: "cs-recorder", traceId: "trace-recorder" }]
  }
];

const namespace = "default";

async function main() {
  const env = await TestWorkflowEnvironment.createTimeSkipping();
  const workers = await Promise.all(
    [...new Set(scenario.map((entry) => `${entry.bundle}|${entry.queue}`))].map((key) => {
      const [bundle, queue] = key.split("|");
      return Worker.create({ connection: env.nativeConnection, taskQueue: queue, workflowsPath: bundle, activities });
    })
  );
  const runs = workers.map((worker) => worker.run());

  await mkdir(fixtures, { recursive: true });

  try {
    for (const entry of scenario) {
      const workflowId = entry.workflowId ?? `${entry.name}-recorder`;
      const handle = await env.client.workflow.start(entry.type, {
        taskQueue: entry.queue, workflowId, args: entry.args
      });
      if (entry.terminateAfterMs) {
        // The supervisor never returns; give it time to record the handover, then stop.
        await new Promise((resolve) => setTimeout(resolve, entry.terminateAfterMs));
        await handle.terminate("recorded for the replay gate");
      } else {
        await handle.result();
      }
      const description = await env.client.workflow.getHandle(workflowId).describe();
      const response = await env.client.workflowService.getWorkflowExecutionHistory({
        namespace,
        execution: { workflowId, runId: description.runId }
      });
      const events = response.history?.events?.length ?? 0;
      await writeFile(join(fixtures, `${entry.name}.json`), `${JSON.stringify(response.history, null, 2)}\n`, "utf8");
      console.log(`recorded ${entry.name}.json (${events} events)`);
    }
  } finally {
    for (const worker of workers) worker.shutdown();
    await Promise.allSettled(runs);
    await env.teardown();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
