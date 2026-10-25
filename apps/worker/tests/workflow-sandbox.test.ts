import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { Worker } from "@temporalio/worker";
import { DEFAULT_TASK_QUEUE } from "../src/constants.js";
import { MEMORY_MAINTENANCE_TASK_QUEUE } from "../src/memory-maintenance-config.js";
import { MEMORY_MUTATION_TASK_QUEUE } from "../src/memory-mutation-config.js";
import { DOT_GOAL_SIGNAL, ROOM_AGENT_ENQUEUE_SIGNAL } from "@space/contracts";
import { runDotGoalLoop } from "../src/dot-goal-loop.js";

/**
 * Real Temporal workflow sandbox (CORE-006).
 *
 * Every workflow in `apps/worker/src` runs here inside
 * `TestWorkflowEnvironment.createTimeSkipping()` with the production workflow
 * code and stubbed activities: no database, no Docker, no AI provider. The
 * mocked suites next to this file keep their own unit-level coverage; this file
 * is the only place the real workflow sandbox, timers, signal ordering and
 * continue-as-new behaviour are exercised.
 */

const MAIN_WORKFLOWS = fileURLToPath(new URL("../src/workflows.ts", import.meta.url));
const MAINTENANCE_WORKFLOWS = fileURLToPath(new URL("../src/memory-maintenance-workflows.ts", import.meta.url));
const MUTATION_WORKFLOWS = fileURLToPath(new URL("../src/memory-mutation-workflows.ts", import.meta.url));

type Call = { name: string; args: unknown[] };
type Handler = (...args: any[]) => unknown;

const calls: Call[] = [];
let handlers: Record<string, Handler> = {};

function stub(name: string) {
  return async (...args: unknown[]) => {
    calls.push({ name, args });
    const handler = handlers[name];
    if (!handler) throw new Error(`workflow called unstubbed activity: ${name}`);
    return handler(...args);
  };
}

const ACTIVITY_NAMES = [
  // workflows.ts
  "recordDummyTurnStarted", "recordDummyTurnCompleted",
  "runCodexAppServerTurn", "runOpenCodeAgentTurn", "runCliAgentTurn", "runRoomAgentBackgroundAction",
  "markRoomAgentMissionStarted", "markRoomAgentMissionFinished", "markRoomAgentMissionContinued",
  "settleRoomAgentBackgroundMission", "checkRoomAgentSupervisorTurn", "discardRoomAgentSupervisorTurn",
  "finishRoomAgentSupervisorTurn", "prepareRoomAgentBackgroundContinuation", "failRoomAgentBackgroundAction",
  "dotGoalStep", "dotGoalObserve",
  // memory-maintenance-workflows.ts
  "refreshMemoryGraphSnapshot", "refreshAllMonthsMemoryGraphSnapshot", "reconcileStaleSpaceAgentSessions",
  "createScheduledMemoryAudit", "prepareMemoryConsolidation", "finalizeMemoryRepair",
  // memory-mutation-workflows.ts
  "executePersistedMemoryChangeSet"
] as const;

function activities() {
  return Object.fromEntries(ACTIVITY_NAMES.map((name) => [name, stub(name)]));
}

function called(name: string): Call[] {
  return calls.filter((call) => call.name === name);
}

let env: TestWorkflowEnvironment;
let workers: Worker[] = [];
let runs: Promise<void>[] = [];

beforeAll(async () => {
  env = await TestWorkflowEnvironment.createTimeSkipping();
  workers = await Promise.all([
    Worker.create({ connection: env.nativeConnection, taskQueue: DEFAULT_TASK_QUEUE, workflowsPath: MAIN_WORKFLOWS, activities: activities() }),
    Worker.create({ connection: env.nativeConnection, taskQueue: MEMORY_MAINTENANCE_TASK_QUEUE, workflowsPath: MAINTENANCE_WORKFLOWS, activities: activities() }),
    Worker.create({ connection: env.nativeConnection, taskQueue: MEMORY_MUTATION_TASK_QUEUE, workflowsPath: MUTATION_WORKFLOWS, activities: activities() })
  ]);
  runs = workers.map((worker) => worker.run());
}, 300_000);

beforeEach(() => {
  calls.length = 0;
  handlers = {};
});

afterAll(async () => {
  for (const worker of workers) worker.shutdown();
  await Promise.allSettled(runs);
  await env?.teardown();
});

let sequence = 0;
function workflowId(prefix: string) {
  sequence += 1;
  return `${prefix}-sandbox-${sequence}`;
}

async function waitFor(predicate: () => boolean, label: string, timeoutMs = 90_000) {
  const startedAt = Date.now();
  while (!predicate()) {
    if (Date.now() - startedAt > timeoutMs) throw new Error(`timed out waiting for ${label}`);
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

/**
 * continueAsNew closes the current run and opens a new one under the same workflow
 * id, so a changed run id is the observable proof that the handover happened. This
 * avoids describing a closed run, which the time-skipping server does not answer.
 */
async function waitForContinuation(
  handle: { describe(): Promise<{ runId: string }> },
  firstRunId: string,
  label: string,
  timeoutMs = 120_000
) {
  const startedAt = Date.now();
  for (;;) {
    const description = await handle.describe();
    if (description.runId !== firstRunId) return description;
    if (Date.now() - startedAt > timeoutMs) throw new Error(`timed out waiting for ${label}; the run id never changed`);
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
}

const TURN: any = {
  roomId: "room-sandbox",
  paneId: "pane-sandbox",
  prompt: "Run the sandbox turn",
  artifactIds: []
};

describe("dummyTurnWorkflow", () => {
  it("records start then completion through real activities", async () => {
    handlers.recordDummyTurnStarted = () => undefined;
    handlers.recordDummyTurnCompleted = (input) => ({ status: "COMPLETED", message: `done:${input.prompt}` });

    const handle = await env.client.workflow.start("dummyTurnWorkflow", {
      taskQueue: DEFAULT_TASK_QUEUE, workflowId: workflowId("dummy"), args: [TURN]
    });
    await expect(handle.result()).resolves.toEqual({ status: "COMPLETED", message: `done:${TURN.prompt}` });
    expect(called("recordDummyTurnStarted")).toHaveLength(1);
    expect(called("recordDummyTurnCompleted")).toHaveLength(1);
  }, 120_000);
});

describe("codexAppServerTurnWorkflow", () => {
  it("routes opencode providers to the OpenCode turn activity", async () => {
    handlers.runOpenCodeAgentTurn = () => ({ status: "COMPLETED", message: "opencode" });
    const handle = await env.client.workflow.start("codexAppServerTurnWorkflow", {
      taskQueue: DEFAULT_TASK_QUEUE, workflowId: workflowId("turn-opencode"),
      args: [{ ...TURN, providerId: "opencode" }]
    });
    await expect(handle.result()).resolves.toEqual({ status: "COMPLETED", message: "opencode" });
    expect(called("runOpenCodeAgentTurn")).toHaveLength(1);
    expect(called("runCodexAppServerTurn")).toHaveLength(0);
  }, 120_000);

  it("routes CLI chat providers to the CLI turn activity", async () => {
    handlers.runCliAgentTurn = () => ({ status: "COMPLETED", message: "cli" });
    const handle = await env.client.workflow.start("codexAppServerTurnWorkflow", {
      taskQueue: DEFAULT_TASK_QUEUE, workflowId: workflowId("turn-cli"),
      args: [{ ...TURN, providerId: "cli:claude" }]
    });
    await expect(handle.result()).resolves.toEqual({ status: "COMPLETED", message: "cli" });
    expect(called("runCliAgentTurn")).toHaveLength(1);
    expect(called("runCodexAppServerTurn")).toHaveLength(0);
  }, 120_000);

  it("retries the turn activity and surfaces the failure to the caller", async () => {
    let attempts = 0;
    handlers.runCodexAppServerTurn = () => {
      attempts += 1;
      throw new Error("provider exploded");
    };
    const handle = await env.client.workflow.start("codexAppServerTurnWorkflow", {
      taskQueue: DEFAULT_TASK_QUEUE, workflowId: workflowId("turn-retry"), args: [TURN]
    });
    await expect(handle.result()).rejects.toThrow();
    expect(attempts).toBeGreaterThan(1);
  }, 120_000);
});

describe("roomAgentActionWorkflow", () => {
  const actionInput = { bridge: { roomId: "room-sandbox", missionId: "mission-sandbox" }, traceId: "trace-sandbox" };

  it("returns the background action result", async () => {
    handlers.runRoomAgentBackgroundAction = () => ({ status: "COMPLETED" });
    const handle = await env.client.workflow.start("roomAgentActionWorkflow", {
      taskQueue: DEFAULT_TASK_QUEUE, workflowId: "space-room-action:action-sandbox-ok", args: [actionInput]
    });
    await expect(handle.result()).resolves.toEqual({ status: "COMPLETED" });
    expect(called("failRoomAgentBackgroundAction")).toHaveLength(0);
  }, 120_000);

  it("persists the failure through the patched failure activity and rethrows", async () => {
    handlers.runRoomAgentBackgroundAction = () => { throw new Error("transport failed"); };
    handlers.failRoomAgentBackgroundAction = () => undefined;
    const handle = await env.client.workflow.start("roomAgentActionWorkflow", {
      taskQueue: DEFAULT_TASK_QUEUE, workflowId: "space-room-action:action-sandbox-fail", args: [actionInput]
    });
    await expect(handle.result()).rejects.toThrow();
    const failures = called("failRoomAgentBackgroundAction");
    expect(failures).toHaveLength(1);
    expect(failures[0]!.args[0]).toMatchObject({
      roomId: "room-sandbox", missionId: "mission-sandbox", actionId: "action-sandbox-fail", cancelled: false
    });
  }, 120_000);
});

describe("memoryMutationWorkflow", () => {
  it("executes the persisted change set", async () => {
    handlers.executePersistedMemoryChangeSet = () => ({ status: "APPLIED" });
    const handle = await env.client.workflow.start("memoryMutationWorkflow", {
      taskQueue: MEMORY_MUTATION_TASK_QUEUE, workflowId: workflowId("mutation"),
      args: [{ changeSetId: "change-set-sandbox", traceId: "trace-sandbox" }]
    });
    await expect(handle.result()).resolves.toEqual({ status: "APPLIED" });
    expect(called("executePersistedMemoryChangeSet")).toHaveLength(1);
  }, 120_000);
});

describe("memoryConsolidationWorkflow", () => {
  it("returns the prepared run without mutating when there is no change set", async () => {
    handlers.prepareMemoryConsolidation = () => ({ changeSetId: null, run: { id: "run-sandbox", status: "NO_CHANGES" } });
    const handle = await env.client.workflow.start("memoryConsolidationWorkflow", {
      taskQueue: MEMORY_MAINTENANCE_TASK_QUEUE, workflowId: workflowId("consolidation-empty"),
      args: [{ runId: "run-sandbox", traceId: "trace-sandbox" }]
    });
    await expect(handle.result()).resolves.toEqual({ id: "run-sandbox", status: "NO_CHANGES" });
    expect(called("executePersistedMemoryChangeSet")).toHaveLength(0);
    expect(called("finalizeMemoryRepair")).toHaveLength(0);
  }, 120_000);

  it("executes and finalizes the change set when the audit produced one", async () => {
    handlers.prepareMemoryConsolidation = () => ({ changeSetId: "cs-1", run: { id: "run-sandbox", status: "PENDING" } });
    handlers.executePersistedMemoryChangeSet = () => ({ status: "APPLIED" });
    handlers.finalizeMemoryRepair = (input) => ({ id: input.runId, status: "REPAIRED", changeSetId: input.changeSetId });
    const handle = await env.client.workflow.start("memoryConsolidationWorkflow", {
      taskQueue: MEMORY_MAINTENANCE_TASK_QUEUE, workflowId: workflowId("consolidation-apply"),
      args: [{ runId: "run-sandbox", traceId: "trace-sandbox" }]
    });
    await expect(handle.result()).resolves.toEqual({ id: "run-sandbox", status: "REPAIRED", changeSetId: "cs-1" });
    expect(called("executePersistedMemoryChangeSet")[0]!.args[0]).toEqual({ changeSetId: "cs-1", traceId: "trace-sandbox" });
    expect(called("finalizeMemoryRepair")).toHaveLength(1);
  }, 120_000);
});

describe("memoryMaintenanceWorkflow", () => {
  const input = { scheduledAt: "2026-10-25T00:00:00.000Z", traceId: "trace-sandbox" };
  const refreshed = { status: "REFRESHED", generatedAt: input.scheduledAt, sourceHash: "hash-1", previousSourceHash: null, summary: {}, durationMs: 12 };

  it("runs consolidation as a child workflow when every step succeeds", async () => {
    handlers.refreshMemoryGraphSnapshot = () => refreshed;
    handlers.refreshAllMonthsMemoryGraphSnapshot = () => undefined;
    handlers.reconcileStaleSpaceAgentSessions = () => undefined;
    handlers.createScheduledMemoryAudit = () => ({ id: "run-ok", workflowId: workflowId("audit") });
    handlers.prepareMemoryConsolidation = () => ({ changeSetId: null, run: { id: "run-ok", status: "NO_CHANGES" } });

    const handle = await env.client.workflow.start("memoryMaintenanceWorkflow", {
      taskQueue: MEMORY_MAINTENANCE_TASK_QUEUE, workflowId: workflowId("maintenance-ok"), args: [input]
    });
    await expect(handle.result()).resolves.toEqual(refreshed);
    expect(called("refreshMemoryGraphSnapshot")).toHaveLength(1);
    expect(called("refreshAllMonthsMemoryGraphSnapshot")).toHaveLength(1);
    expect(called("reconcileStaleSpaceAgentSessions")).toHaveLength(1);
    expect(called("createScheduledMemoryAudit")).toHaveLength(1);
    // The child workflow really executed in the sandbox: it calls its own activity.
    expect(called("prepareMemoryConsolidation")).toHaveLength(1);
  }, 180_000);

  it("continues when the all-months refresh and the session reconciliation both fail", async () => {
    handlers.refreshMemoryGraphSnapshot = () => refreshed;
    handlers.refreshAllMonthsMemoryGraphSnapshot = () => { throw new Error("all-months refresh failed"); };
    handlers.reconcileStaleSpaceAgentSessions = () => { throw new Error("reconciliation failed"); };
    handlers.createScheduledMemoryAudit = () => ({ id: "run-degraded", workflowId: workflowId("audit-degraded") });
    handlers.prepareMemoryConsolidation = () => ({ changeSetId: null, run: { id: "run-degraded", status: "NO_CHANGES" } });

    const handle = await env.client.workflow.start("memoryMaintenanceWorkflow", {
      taskQueue: MEMORY_MAINTENANCE_TASK_QUEUE, workflowId: workflowId("maintenance-degraded"), args: [input]
    });
    await expect(handle.result()).resolves.toEqual(refreshed);
    // Both failure paths were retried up to their maximum attempts and then swallowed.
    expect(called("refreshAllMonthsMemoryGraphSnapshot").length).toBeGreaterThan(1);
    expect(called("reconcileStaleSpaceAgentSessions").length).toBeGreaterThan(1);
    expect(called("createScheduledMemoryAudit")).toHaveLength(1);
    expect(called("prepareMemoryConsolidation")).toHaveLength(1);
  }, 180_000);

  it("fails the workflow when the live snapshot refresh itself fails", async () => {
    handlers.refreshMemoryGraphSnapshot = () => { throw new Error("snapshot refresh failed"); };

    const handle = await env.client.workflow.start("memoryMaintenanceWorkflow", {
      taskQueue: MEMORY_MAINTENANCE_TASK_QUEUE, workflowId: workflowId("maintenance-fail"), args: [input]
    });
    await expect(handle.result()).rejects.toThrow();
    expect(called("refreshMemoryGraphSnapshot").length).toBeGreaterThan(1);
    expect(called("createScheduledMemoryAudit")).toHaveLength(0);
  }, 180_000);
});

describe("dotGoalWorkflow", () => {
  it("returns the goal status as soon as the API reports DONE", async () => {
    handlers.dotGoalStep = () => ({ state: "DONE", runId: null, goalStatus: "COMPLETE" });

    const handle = await env.client.workflow.start("dotGoalWorkflow", {
      taskQueue: DEFAULT_TASK_QUEUE, workflowId: workflowId("dot-done"), args: [{ goalId: "goal-sandbox" }]
    });
    await expect(handle.result()).resolves.toEqual({ goalStatus: "COMPLETE" });
    expect(called("dotGoalStep")).toHaveLength(1);
    expect(called("dotGoalObserve")).toHaveLength(0);
  }, 120_000);

  it("ends the wait early when the wake signal arrives instead of the timeout", async () => {
    let steps = 0;
    handlers.dotGoalStep = () => {
      steps += 1;
      return steps === 1
        ? { state: "WAITING", runId: null, goalStatus: "RUNNING" }
        : { state: "DONE", runId: null, goalStatus: "COMPLETE" };
    };

    const id = workflowId("dot-wake");
    const handle = await env.client.workflow.start("dotGoalWorkflow", {
      taskQueue: DEFAULT_TASK_QUEUE, workflowId: id, args: [{ goalId: "goal-sandbox" }]
    });
    // The workflow is idle in its 30 second wait; a wake-up must end it at once.
    await waitFor(() => called("dotGoalStep").length === 1, "the first goal step");
    await handle.signal(DOT_GOAL_SIGNAL);
    await expect(handle.result()).resolves.toEqual({ goalStatus: "COMPLETE" });
    expect(called("dotGoalStep")).toHaveLength(2);
  }, 120_000);

  it("polls the observed run until it stops being QUEUED or RUNNING", async () => {
    let observations = 0;
    handlers.dotGoalStep = () => ({ state: "STARTED", runId: "run-sandbox-step", goalStatus: "RUNNING" });
    handlers.dotGoalObserve = () => {
      observations += 1;
      return observations < 3
        ? { run: "RUNNING", goalStatus: "RUNNING" }
        : { run: "SUCCEEDED", goalStatus: "RUNNING" };
    };
    // The final step reports DONE so the loop can return.
    const originalStep = handlers.dotGoalStep;
    handlers.dotGoalStep = (goalId: string) => {
      if (observations >= 3) return { state: "DONE", runId: null, goalStatus: "COMPLETE" };
      return originalStep(goalId);
    };

    const handle = await env.client.workflow.start("dotGoalWorkflow", {
      taskQueue: DEFAULT_TASK_QUEUE, workflowId: workflowId("dot-observe"), args: [{ goalId: "goal-sandbox" }]
    });
    await expect(handle.result()).resolves.toEqual({ goalStatus: "COMPLETE" });
    expect(called("dotGoalObserve")).toHaveLength(3);
  }, 120_000);

  it("continues as new while the goal is still unfinished", async () => {
    // The live handover needs the workflow to burn its full 300-iteration budget,
    // and every iteration is a real activity round trip (minutes, not seconds).
    // That would blow the CI budget this issue caps at three minutes, so the live
    // continuation of this workflow is covered by a recorded history in
    // tests/workflow-histories. Replay drives the same code path without executing
    // 300 activities, and `runDotGoalLoop` reports the unfinished goal directly.
    let steps = 0;
    const result = await runDotGoalLoop("goal-budget-sandbox", {
      step: async () => { steps += 1; return { state: "WAITING", runId: null, goalStatus: "RUNNING" }; },
      observe: async () => ({ run: "SUCCEEDED", goalStatus: "RUNNING" }),
      wait: async () => undefined
    }, 3);
    // `goalStatus === null` is exactly what makes dotGoalWorkflow continue as new.
    expect(result).toEqual({ goalStatus: null });
    expect(steps).toBe(3);
  }, 30_000);

  it("stops the loop as soon as the history says to continue as new", async () => {
    let steps = 0;
    const result = await runDotGoalLoop("goal-history-sandbox", {
      step: async () => { steps += 1; return { state: "WAITING", runId: null, goalStatus: "RUNNING" }; },
      observe: async () => ({ run: "SUCCEEDED", goalStatus: "RUNNING" }),
      wait: async () => undefined,
      shouldContinueAsNew: () => true
    }, 300);
    expect(result).toEqual({ goalStatus: null });
    expect(steps).toBe(0);
  }, 30_000);
});

describe("roomAgentSupervisorWorkflow", () => {
  const ROOM = "room-sandbox";
  const queueItem = (missionId: string, agentRunId: string) => ({
    missionId,
    turn: {
      roomId: ROOM, paneId: "pane-sandbox", prompt: "Run the sandbox mission",
      artifactIds: [], agentRunId, traceId: `trace-${agentRunId}-sandbox`
    }
  });
  const supervisorInput = (overrides: Record<string, unknown> = {}) => ({
    roomId: ROOM, pending: [], monitoringMissionIds: [], interrupted: [], processedCount: 0, ...overrides
  });
  const verifiedTurn = () => ({
    status: "COMPLETED", message: "ok",
    roomAgentOutcome: { status: "VERIFIED", executedActionCount: 0, statusReason: "Verified." }
  });

  beforeEach(() => {
    handlers.checkRoomAgentSupervisorTurn = () => "PROCEED";
    handlers.discardRoomAgentSupervisorTurn = () => undefined;
    handlers.finishRoomAgentSupervisorTurn = () => undefined;
    handlers.runCodexAppServerTurn = verifiedTurn;
  });

  it("runs a queued turn through the child workflow and finishes the mission", async () => {
    const handle = await env.client.workflow.start("roomAgentSupervisorWorkflow", {
      taskQueue: DEFAULT_TASK_QUEUE, workflowId: workflowId("supervisor-run"),
      args: [supervisorInput({ pending: [queueItem("mission-run", "agent-run")] })]
    });
    await waitFor(() => called("finishRoomAgentSupervisorTurn").length === 1, "the mission to finish");
    expect(called("checkRoomAgentSupervisorTurn")).toHaveLength(1);
    expect(called("runCodexAppServerTurn")).toHaveLength(1);
    expect(called("finishRoomAgentSupervisorTurn")[0]!.args[0]).toMatchObject({ status: "COMPLETED" });
    await handle.terminate("sandbox test finished");
  }, 180_000);

  it("accepts a new mission through the enqueue signal once the queue is idle", async () => {
    const handle = await env.client.workflow.start("roomAgentSupervisorWorkflow", {
      taskQueue: DEFAULT_TASK_QUEUE, workflowId: workflowId("supervisor-signal"),
      args: [supervisorInput({ pending: [queueItem("mission-first", "agent-run-first")] })]
    });
    await waitFor(() => called("finishRoomAgentSupervisorTurn").length === 1, "the first mission to finish");
    // The supervisor is now parked on a signal condition with its handlers registered.
    await handle.signal(ROOM_AGENT_ENQUEUE_SIGNAL, queueItem("mission-second", "agent-run-second"));
    await waitFor(() => called("finishRoomAgentSupervisorTurn").length === 2, "the signalled mission to finish");
    expect(called("checkRoomAgentSupervisorTurn")).toHaveLength(2);
    expect(called("runCodexAppServerTurn")).toHaveLength(2);
    await handle.terminate("sandbox test finished");
  }, 180_000);

  it("discards and interrupts a turn carried in the interrupted list", async () => {
    const handle = await env.client.workflow.start("roomAgentSupervisorWorkflow", {
      taskQueue: DEFAULT_TASK_QUEUE, workflowId: workflowId("supervisor-interrupted"),
      args: [supervisorInput({ interrupted: [{ item: queueItem("mission-stop", "agent-run-stop"), reason: "Stopped by operator." }] })]
    });
    await waitFor(() => called("finishRoomAgentSupervisorTurn").length === 1, "the interrupted turn to be retired");
    expect(called("discardRoomAgentSupervisorTurn")).toHaveLength(1);
    expect(called("finishRoomAgentSupervisorTurn")[0]!.args[0]).toMatchObject({ status: "INTERRUPTED", statusReason: "Stopped by operator." });
    expect(called("runCodexAppServerTurn")).toHaveLength(0);
    await handle.terminate("sandbox test finished");
  }, 180_000);

  it("skips a turn the API reports as no longer active", async () => {
    handlers.checkRoomAgentSupervisorTurn = () => "SKIP";
    const handle = await env.client.workflow.start("roomAgentSupervisorWorkflow", {
      taskQueue: DEFAULT_TASK_QUEUE, workflowId: workflowId("supervisor-skip"),
      args: [supervisorInput({ pending: [queueItem("mission-skip", "agent-run-skip")] })]
    });
    await waitFor(() => called("discardRoomAgentSupervisorTurn").length === 1, "the skipped turn to be discarded");
    expect(called("runCodexAppServerTurn")).toHaveLength(0);
    expect(called("finishRoomAgentSupervisorTurn")).toHaveLength(0);
    await handle.terminate("sandbox test finished");
  }, 180_000);

  it("records a failed turn with the supervisor failure code", async () => {
    handlers.runCodexAppServerTurn = () => { throw new Error("child failed"); };
    const handle = await env.client.workflow.start("roomAgentSupervisorWorkflow", {
      taskQueue: DEFAULT_TASK_QUEUE, workflowId: workflowId("supervisor-fail"),
      args: [supervisorInput({ pending: [queueItem("mission-fail", "agent-run-fail")] })]
    });
    await waitFor(() => called("finishRoomAgentSupervisorTurn").length === 1, "the failed turn to be recorded");
    expect(called("discardRoomAgentSupervisorTurn")).toHaveLength(1);
    expect(called("finishRoomAgentSupervisorTurn")[0]!.args[0]).toMatchObject({
      status: "FAILED", errorCode: "ROOM_AGENT_SUPERVISOR_FAILED"
    });
    await handle.terminate("sandbox test finished");
  }, 180_000);

  it("continues as new once the processed count reaches the carry bound", async () => {
    handlers.checkRoomAgentSupervisorTurn = () => "SKIP";
    const id = workflowId("supervisor-can");
    const handle = await env.client.workflow.start("roomAgentSupervisorWorkflow", {
      taskQueue: DEFAULT_TASK_QUEUE, workflowId: id,
      args: [supervisorInput({ pending: [queueItem("mission-carry", "agent-run-carry")], processedCount: 49 })]
    });
    const firstRunId = handle.firstExecutionRunId;
    const latest = env.client.workflow.getHandle(id);
    const continued = await waitForContinuation(latest, firstRunId, "the supervisor run to continue as new");
    expect(continued.runId).not.toBe(firstRunId);
    // The carried queue and counter are handed to the next run, which idles on its signals.
    await handle.terminate("sandbox test finished");
  }, 180_000);
});
