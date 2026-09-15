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

import { afterEach, describe, expect, expectTypeOf, it, vi } from "vitest";
import { AIRFLOW_METADATA_FLAG } from "../src/coordinator/manifest.js";
import type {
  ConnectionResult,
  DagSpec,
  GetXComOpts,
  SetXComOpts,
  TaskClient,
  TaskContext,
  TaskHandler,
  TaskInputs,
  TaskOptions,
  TaskRef,
  TaskSpec,
} from "../src/index.js";
import * as sdk from "../src/index.js";
import {
  ConnectionNotFoundError,
  Dag,
  DagRegistry,
  serveDags,
  SUPERVISOR_API_VERSION,
  VariableNotFoundError,
} from "../src/index.js";

describe("public API", () => {
  it("exports the Dag authoring surface", async () => {
    const dag = new Dag("public_api_dag");
    const upstream = dag.task("public_api_task", async () => undefined);
    const downstream = dag.task("public_api_downstream", async () => undefined, {
      inputs: { upstream },
    });
    expect(upstream).toEqual({ dagId: "public_api_dag", taskId: "public_api_task" });
    expect(downstream).toEqual({ dagId: "public_api_dag", taskId: "public_api_downstream" });
    expect(dag.taskIds).toEqual(["public_api_task", "public_api_downstream"]);
    // serveDags hands the registry to the runtime, which needs the supervisor's
    // socket addresses that Airflow puts on argv.
    await expect(serveDags(new DagRegistry(dag))).rejects.toThrow("Missing --comm");
  });

  // The registry guard runs before the already-served latch, so this holds
  // regardless of whether another test in this file already served a registry.
  it.each([
    ["a bare Dag", new Dag("not_a_registry_dag")],
    ["a plain object", { register: () => {} }],
    ["null", null],
  ])("rejects a %s in place of a registry", async (_label, value) => {
    await expect(serveDags(value as unknown as DagRegistry)).rejects.toThrow(
      /serveDags\(\.\.\.\) takes a DagRegistry/,
    );
  });

  it("names the duplicate-copy cause for a registry built by another copy", async () => {
    // Stands in for a registry from a second resolved copy: same brand, other
    // class. It still cannot be served, so the point is only that it says why.
    const foreign = {};
    Object.defineProperty(foreign, Symbol.for("airflow.ts-sdk.DagRegistry"), { value: true });
    await expect(serveDags(foreign as unknown as DagRegistry)).rejects.toThrow(
      /different copy of apache-airflow-ts-sdk/,
    );
  });

  describe("the one-shot serve latch", () => {
    // Global by design, so it outlives the test that trips it.
    afterEach(() => {
      delete (globalThis as unknown as Record<symbol, unknown>)[
        Symbol.for("airflow.ts-sdk.served")
      ];
    });

    it("rejects a second call once a serve has completed", async () => {
      const argv = process.argv;
      // The one path that completes without sockets.
      process.argv = [...argv, AIRFLOW_METADATA_FLAG];
      vi.spyOn(process.stdout, "write").mockReturnValue(true);
      try {
        await serveDags(new DagRegistry(new Dag("served_dag")));
        await expect(serveDags(new DagRegistry(new Dag("second_call_dag")))).rejects.toThrow(
          /serveDags\(\.\.\.\) was already called/,
        );
      } finally {
        process.argv = argv;
        vi.restoreAllMocks();
      }
    });

    it("releases the latch when a serve fails, so the call can be retried", async () => {
      await expect(serveDags(new DagRegistry(new Dag("first_try")))).rejects.toThrow(
        "Missing --comm",
      );
      // The retry reports why it actually failed, not "already called".
      await expect(serveDags(new DagRegistry(new Dag("second_try")))).rejects.toThrow(
        "Missing --comm",
      );
    });
  });

  it("exports DagRegistry as the Dag collection a bundle serves", () => {
    const dag = new Dag("registry_api_dag");
    const handler = async () => "hello";
    dag.task("extract", handler);
    // Building a registry starts nothing, so a test can dispatch through it
    // exactly as the runtime does, with no sockets in scope.
    const registry = new DagRegistry(dag);
    expect(registry.getTaskHandler("registry_api_dag", "extract")).toBe(handler);
    registry.register(new Dag("late_dag"));
    expect(registry.getTaskHandler("late_dag", "extract")).toBeUndefined();
  });

  it("keeps registry enumeration out of the public surface", () => {
    const registry = new DagRegistry();
    for (const name of ["listTasks", "listDags"]) {
      expect(name in registry).toBe(false);
    }
    expectTypeOf<keyof DagRegistry>().toEqualTypeOf<"register" | "getTaskHandler">();
  });

  it("does not export the removed registerTask surface or the coordinator itself", () => {
    for (const name of [
      "registerTask",
      "listRegisteredTasks",
      "registerDags",
      "defaultRegistry",
      "startCoordinator",
    ]) {
      expect(name in sdk).toBe(false);
    }
    expectTypeOf<typeof sdk>().not.toHaveProperty("registerTask");
    expectTypeOf<typeof sdk>().not.toHaveProperty("listRegisteredTasks");
    expectTypeOf<typeof sdk>().not.toHaveProperty("registerDags");
    // The runtime reads the registry it is handed, so there is no process-wide
    // registry for a Dag constructor to write into.
    expectTypeOf<typeof sdk>().not.toHaveProperty("defaultRegistry");
    expectTypeOf<typeof sdk>().not.toHaveProperty("startCoordinator");
  });

  it("exports public error classes", () => {
    const err = new VariableNotFoundError("missing");
    expect(err).toBeInstanceOf(Error);
    expect(err.name).toBe("VariableNotFoundError");
    expect(err.key).toBe("missing");

    const connErr = new ConnectionNotFoundError("missing_conn");
    expect(connErr).toBeInstanceOf(Error);
    expect(connErr.name).toBe("ConnectionNotFoundError");
    expect(connErr.connId).toBe("missing_conn");
  });

  it("reaches the runtime only through serveDags, which takes a registry", () => {
    expectTypeOf<typeof serveDags>().toEqualTypeOf<(registry: DagRegistry) => Promise<void>>();
    expectTypeOf<ConstructorParameters<typeof DagRegistry>>().toEqualTypeOf<Dag[]>();
    expectTypeOf(SUPERVISOR_API_VERSION).toMatchTypeOf<string>();
  });

  it("keeps the Dag authoring signatures extensible via trailing specs", () => {
    expectTypeOf<TaskRef>().toEqualTypeOf<{
      readonly dagId: string;
      readonly taskId: string;
    }>();
    expectTypeOf<TaskInputs>().toEqualTypeOf<Readonly<Record<string, TaskRef>>>();
    expectTypeOf<TaskOptions>().toEqualTypeOf<{
      readonly inputs?: TaskInputs;
      readonly spec?: TaskSpec;
    }>();
    expectTypeOf<ConstructorParameters<typeof Dag>>().toEqualTypeOf<[string, DagSpec?]>();
    expectTypeOf<Dag["task"]>().toEqualTypeOf<
      <TReturn = unknown>(
        taskId: string,
        handler: TaskHandler<TReturn>,
        options?: TaskOptions,
      ) => TaskRef
    >();
    expectTypeOf<Dag["taskIds"]>().toEqualTypeOf<readonly string[]>();
    // Reserved with no fields yet, so only `{}` is expressible. Generated specs
    // will be all-optional (weak) types, and `{}` stays assignable to those, so
    // filling these in later cannot break a call site.
    expectTypeOf<DagSpec>().toEqualTypeOf<Record<string, never>>();
    expectTypeOf<TaskSpec>().toEqualTypeOf<Record<string, never>>();
  });

  it("uses idiomatic TypeScript names for public client types", () => {
    expectTypeOf<TaskContext>().toEqualTypeOf<{
      readonly dagId: string;
      readonly taskId: string;
      readonly runId: string;
      readonly tryNumber: number;
      readonly mapIndex: number;
      readonly hasMappedDependants: boolean;
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
    expectTypeOf<TaskClient["getConnectionOrThrow"]>().toEqualTypeOf<
      (connId: string) => Promise<ConnectionResult>
    >();
    expectTypeOf<TaskClient["getXCom"]>().toEqualTypeOf<
      <T = unknown>(opts: GetXComOpts) => Promise<T | null>
    >();
  });

  it("rejects wire-format names and non-JSON XCom values", () => {
    function acceptsGetXComOpts(_opts: GetXComOpts): void {}
    function acceptsSetXComOpts(_opts: SetXComOpts): void {}

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
    // Never invoked: these constructor/method misuses also throw at runtime.
    const rejectsPositionalMisuse = () => {
      // @ts-expect-error dagId is positional, not an options object.
      new Dag({ dagId: "example" });
      // @ts-expect-error a task handler is required.
      new Dag("example").task("extract");
      const dag = new Dag("example");
      const upstream = dag.task("extract", async () => undefined);
      // @ts-expect-error inputs must be task handles, not arbitrary values.
      dag.task("transform", async () => undefined, { inputs: { count: 1 } });
      // @ts-expect-error inputs and spec are keyword-only, not positional.
      dag.task("transform2", async () => undefined, { upstream });
      // @ts-expect-error a Dag spec is an options object, not a primitive.
      new Dag("spec_dag", 42);
      // @ts-expect-error DagSpec has no fields yet, so a schedule cannot be declared here.
      new Dag("spec_dag", { schedule: "@daily" });
      // @ts-expect-error TaskSpec has no fields yet, so retries cannot be declared here.
      dag.task("transform3", async () => undefined, { spec: { retries: 2 } });
      // @ts-expect-error the TaskRef handle is data, not callable.
      upstream();
      // @ts-expect-error serveDags takes the registry, not a bare Dag.
      serveDags(dag);
      // @ts-expect-error a registry is built from Dags, not from task handles.
      new DagRegistry(upstream);
    };
    void rejectsPositionalMisuse;
    // @ts-expect-error the TaskRef handle is opaque and does not expose the handler.
    expectTypeOf<TaskRef>().toHaveProperty("handler");
    // @ts-expect-error XCom values must be JSON-compatible.
    acceptsSetXComOpts({ key: "result", value: new Date() });
  });
});
