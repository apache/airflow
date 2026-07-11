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

import { describe, expect, expectTypeOf, it } from "vitest";
import type {
  ConnectionResult,
  GetXComOpts,
  SetXComOpts,
  StartCoordinatorOptions,
  TaskClient,
  TaskContext,
  TaskRegistration,
} from "../src/index.js";
import {
  listRegisteredTasks,
  registerTask,
  startCoordinator,
  SUPERVISOR_API_VERSION,
  VariableNotFoundError,
} from "../src/index.js";

describe("public API", () => {
  it("exports task registration helpers", () => {
    const registration = { dagId: "public_api_dag", taskId: "public_api_task" };
    registerTask(registration, async () => undefined);
    expect(listRegisteredTasks()).toContainEqual(registration);
  });

  it("exports public error classes", () => {
    const err = new VariableNotFoundError("missing");
    expect(err).toBeInstanceOf(Error);
    expect(err.name).toBe("VariableNotFoundError");
    expect(err.key).toBe("missing");
  });

  it("exports the coordinator runtime entrypoint", () => {
    expectTypeOf<typeof startCoordinator>().toEqualTypeOf<
      (opts?: StartCoordinatorOptions) => Promise<void>
    >();
    expectTypeOf<StartCoordinatorOptions>().toEqualTypeOf<{
      commAddr?: string;
      logsAddr?: string;
      argv?: readonly string[];
    }>();
    expectTypeOf(SUPERVISOR_API_VERSION).toMatchTypeOf<string>();
  });

  it("uses idiomatic TypeScript names for public client types", () => {
    expectTypeOf<TaskRegistration>().toEqualTypeOf<{
      readonly dagId: string;
      readonly taskId: string;
    }>();
    expectTypeOf<TaskContext>().toEqualTypeOf<{
      readonly dagId: string;
      readonly taskId: string;
      readonly runId: string;
      readonly tryNumber: number;
      readonly mapIndex: number;
      readonly signal: AbortSignal;
    }>();
    expectTypeOf<GetXComOpts>().toEqualTypeOf<{
      key: string;
      dagId?: string;
      runId?: string;
      taskId?: string;
      mapIndex?: number | null;
      includePriorDates?: boolean;
    }>();
    expectTypeOf<SetXComOpts>().toEqualTypeOf<{
      key: string;
      value: SetXComOpts["value"];
      dagId?: string;
      runId?: string;
      taskId?: string;
      mapIndex?: number | null;
    }>();
    expectTypeOf<ConnectionResult>().toEqualTypeOf<{
      id: string;
      type: string;
      host?: string | null;
      schema?: string | null;
      login?: string | null;
      password?: string | null;
      port?: number | null;
      extra?: string | null;
    }>();
    expectTypeOf<TaskClient["getConnection"]>().toEqualTypeOf<
      (connId: string) => Promise<ConnectionResult | null>
    >();
    expectTypeOf<TaskClient["getXCom"]>().toEqualTypeOf<
      <T = unknown>(opts: GetXComOpts) => Promise<T | null>
    >();
  });

  it("rejects wire-format names and non-JSON XCom values", () => {
    function acceptsGetXComOpts(_opts: GetXComOpts): void {}
    function acceptsSetXComOpts(_opts: SetXComOpts): void {}
    function acceptsTaskRegistration(_registration: TaskRegistration): void {}

    acceptsGetXComOpts({
      key: "result",
      dagId: "example",
      runId: "manual__2026-01-01T00:00:00+00:00",
      taskId: "extract",
      mapIndex: 0,
      includePriorDates: true,
    });
    acceptsSetXComOpts({
      key: "result",
      value: { count: 1 },
      dagId: "example",
      runId: "manual__2026-01-01T00:00:00+00:00",
      taskId: "extract",
      mapIndex: null,
    });

    // @ts-expect-error public options use dagId, not dag_id.
    acceptsGetXComOpts({ key: "result", dag_id: "example" });
    // @ts-expect-error public options use includePriorDates, not include_prior_dates.
    acceptsGetXComOpts({ key: "result", include_prior_dates: true });
    // @ts-expect-error public ConnectionResult uses id/type, not wire-format names.
    expectTypeOf<ConnectionResult>().toEqualTypeOf<{ conn_id: string; conn_type: string }>();
    // @ts-expect-error public ConnectionResult uses id/type, not connId/connType.
    expectTypeOf<ConnectionResult>().toEqualTypeOf<{ connId: string; connType: string }>();
    // @ts-expect-error public TaskContext does not expose the raw task-instance id.
    expectTypeOf<TaskContext>().toHaveProperty("taskInstanceId");
    // @ts-expect-error task registration requires explicit dagId/taskId fields.
    acceptsTaskRegistration("public_api_task");
    // @ts-expect-error task registration uses dagId, not dag_id.
    acceptsTaskRegistration({ dag_id: "example", taskId: "extract" });
    // @ts-expect-error XCom values must be JSON-compatible.
    acceptsSetXComOpts({ key: "result", value: new Date() });
  });
});
