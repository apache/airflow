/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { afterEach, describe, expect, it, vi } from "vitest";

import type { CoordinatorClient } from "../../src/coordinator/client.js";
import type { LogChannel } from "../../src/coordinator/log-channel.js";
import { buildOperatorContext, runOperator } from "../../src/coordinator/operator-runner.js";
import type { StartupDetails } from "../../src/coordinator/protocol.js";
import { Dag } from "../../src/sdk/dag.js";
import type { Operator, OperatorContext, OperatorOutcome } from "../../src/sdk/operator.js";
import type { TaskContext } from "../../src/sdk/task.js";

const SUCCEEDED = {
  type: "SucceedTask",
  end_date: "2026-10-09T00:00:00Z",
  task_outlets: [],
  outlet_events: [],
} satisfies OperatorOutcome;

function makeDetails(tiContext: Record<string, unknown> = {}): StartupDetails {
  return {
    ti: { id: "ti-1", queue: "typescript" },
    ti_context: tiContext,
  } as unknown as StartupDetails;
}

function makeContext(tiContext: Record<string, unknown> = {}) {
  const setXCom = vi.fn(async () => undefined);
  const skipDownstreamTasks = vi.fn(async () => undefined);
  const error = vi.fn();
  const fail = vi.fn((message: string) => ({ type: "TaskState", state: "failed", message }));
  const op = buildOperatorContext({
    details: makeDetails(tiContext),
    dag: new Dag("d"),
    ctx: { taskId: "t", signal: new AbortController().signal } as TaskContext,
    client: { setXCom, skipDownstreamTasks } as unknown as CoordinatorClient,
    logs: { error } as unknown as LogChannel,
    fail: fail as unknown as OperatorContext["fail"],
  });
  return { op, setXCom, skipDownstreamTasks, error, fail };
}

function makeOperator(overrides: Partial<Operator<never, unknown>> = {}) {
  const execute = vi.fn(async () => SUCCEEDED);
  const executeComplete = vi.fn(async () => SUCCEEDED);
  const operator: Operator<never, unknown> = {
    operatorName: "TestOperator",
    execute,
    executeComplete,
    ...overrides,
  };
  return { operator, execute, executeComplete };
}

describe("runOperator", () => {
  it("starts the operator when the task is not resuming", async () => {
    const { op } = makeContext();
    const { operator, execute, executeComplete } = makeOperator();

    expect(await runOperator(operator, op)).toBe(SUCCEEDED);
    expect(execute).toHaveBeenCalledWith(op);
    expect(executeComplete).not.toHaveBeenCalled();
  });

  it("resumes with the event of execute_complete", async () => {
    const { op } = makeContext({ next_method: "execute_complete", next_kwargs: { event: [1] } });
    const { operator, execute, executeComplete } = makeOperator();

    expect(await runOperator(operator, op)).toBe(SUCCEEDED);
    expect(executeComplete).toHaveBeenCalledWith(op, [1]);
    expect(execute).not.toHaveBeenCalled();
  });

  it("fails with Airflow's reason when Airflow could not resume the task", async () => {
    const { op, error, fail } = makeContext({
      next_method: "__fail__",
      next_kwargs: { error: "Trigger timeout", traceback: ["Traceback", "TimeoutError"] },
    });
    const { operator, execute, executeComplete } = makeOperator();

    await runOperator(operator, op);

    expect(error).toHaveBeenCalledWith("Task could not be resumed:\nTraceback\nTimeoutError");
    expect(fail).toHaveBeenCalledWith("Trigger timeout");
    expect(execute).not.toHaveBeenCalled();
    expect(executeComplete).not.toHaveBeenCalled();
  });

  it.each([
    ["a next_method it does not know", { next_method: "poll" }, {}],
    [
      "execute_complete for an operator that cannot resume",
      { next_method: "execute_complete" },
      { executeComplete: undefined },
    ],
  ])("fails on %s", async (_label, tiContext, overrides) => {
    const { op, fail } = makeContext(tiContext);
    const { operator } = makeOperator(overrides);

    await runOperator(operator, op);

    expect(fail).toHaveBeenCalledWith(
      `Task cannot resume with next_method "${tiContext.next_method}"`,
    );
  });
});

describe("buildOperatorContext", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("succeeds without pushing a result when none is given", async () => {
    const { op, setXCom } = makeContext();

    expect(await op.succeed()).toMatchObject({ type: "SucceedTask", task_outlets: [] });
    expect(setXCom).not.toHaveBeenCalled();
  });

  it("pushes the result under return_value before succeeding", async () => {
    const { op, setXCom } = makeContext();

    await op.succeed({ ok: true });

    expect(setXCom).toHaveBeenCalledWith({ key: "return_value", value: { ok: true } });
  });

  it("skips tasks as SkipMixin does", async () => {
    const { op, setXCom, skipDownstreamTasks } = makeContext();

    await op.skip(["a", "b"]);

    expect(setXCom).toHaveBeenCalledWith({
      key: "skipmixin_key",
      value: { skipped: ["a", "b"] },
    });
    expect(skipDownstreamTasks).toHaveBeenCalledWith(["a", "b"]);
  });

  it.each([
    [undefined, null],
    [30, "PT30S"],
  ])("parks with a timeout of %s as %s", (timeoutSeconds, timeout) => {
    const { op } = makeContext();

    expect(op.awaitInput({ timeoutSeconds })).toEqual({
      type: "AwaitInputTask",
      state: "awaiting_input",
      timeout,
      next_method: "execute_complete",
      next_kwargs: {},
    });
  });

  it("hands the next run the kwargs it parked with", () => {
    const { op } = makeContext();

    expect(op.awaitInput({ kwargs: { attempt: 1 } }).next_kwargs).toEqual({ attempt: 1 });
  });

  it("defers to the trigger, resuming through execute_complete", () => {
    const { op } = makeContext();

    expect(op.defer({ classpath: "pkg.Trigger", kwargs: { a: 1 }, timeoutSeconds: 5 })).toEqual({
      type: "DeferTask",
      state: "deferred",
      classpath: "pkg.Trigger",
      trigger_kwargs: { a: 1 },
      trigger_timeout: "PT5S",
      queue: null,
      next_method: "execute_complete",
      next_kwargs: {},
    });
  });

  it.each([
    ["false", undefined, null],
    ["true", undefined, "typescript"],
    ["true", "other", "other"],
    ["true", null, null],
  ])(
    "with AIRFLOW__TRIGGERER__QUEUES_ENABLED=%s and a queue of %s, defers on %s",
    (enabled, queue, expected) => {
      vi.stubEnv("AIRFLOW__TRIGGERER__QUEUES_ENABLED", enabled);
      const { op } = makeContext();

      expect(op.defer({ classpath: "pkg.Trigger", kwargs: {}, queue }).queue).toBe(expected);
    },
  );
});
