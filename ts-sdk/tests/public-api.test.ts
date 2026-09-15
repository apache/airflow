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
  ArgNameMap,
  ConnectionResult,
  DagSpec,
  GetXComOpts,
  SetXComOpts,
  TaskClient,
  Registerable,
  TaskContext,
  TaskFunction,
  TaskInputs,
  TaskOptions,
  TaskRef,
  TaskSpec,
} from "../src/index.js";
import * as sdk from "../src/index.js";
import {
  Bundle,
  ConnectionNotFoundError,
  Dag,
  getClient,
  getContext,
  SUPERVISOR_API_VERSION,
  TaskHandler,
  VariableNotFoundError,
  withArgNames,
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
    // serve() hands the bundle to the runtime, which needs the supervisor's
    // socket addresses that Airflow puts on argv.
    await expect(new Bundle(dag).serve()).rejects.toThrow("Missing --comm");
  });

  // A detached `const { serve } = bundle` must say so here rather than fail
  // deep in the runtime. The guard runs before the already-served latch, so
  // this holds however many bundles earlier tests in this file served.
  it.each([
    ["a bare Dag", new Dag("not_a_bundle_dag")],
    ["a plain object", { register: () => {} }],
    ["null", null],
    ["undefined, as a detached serve receives", undefined],
  ])("rejects %s as the receiver of serve()", async (_label, value) => {
    const detached = Bundle.prototype.serve;
    await expect(detached.call(value as unknown as Bundle)).rejects.toThrow(
      /bundle\.serve\(\) must be called on a Bundle/,
    );
  });

  it("names the duplicate-copy cause for a bundle built by another copy", async () => {
    // Stands in for a bundle from a second resolved copy: same brand, other
    // class. It still cannot be served, so the point is only that it says why.
    const foreign = {};
    Object.defineProperty(foreign, Symbol.for("airflow.ts-sdk.Bundle"), { value: true });
    const detached = Bundle.prototype.serve;
    await expect(detached.call(foreign as unknown as Bundle)).rejects.toThrow(
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
        await new Bundle(new Dag("served_dag")).serve();
        await expect(new Bundle(new Dag("second_call_dag")).serve()).rejects.toThrow(
          /bundle\.serve\(\) was already called/,
        );
      } finally {
        process.argv = argv;
        vi.restoreAllMocks();
      }
    });

    it("releases the latch when a serve fails, so the call can be retried", async () => {
      await expect(new Bundle(new Dag("first_try")).serve()).rejects.toThrow("Missing --comm");
      // The retry reports why it actually failed, not "already called".
      await expect(new Bundle(new Dag("second_try")).serve()).rejects.toThrow("Missing --comm");
    });
  });

  it("exports Bundle as the thing that holds what a bundle provides and serves it", () => {
    const dag = new Dag("bundle_api_dag");
    const handler = async () => "hello";
    dag.task("extract", handler);
    // Registering starts nothing, so a test can dispatch through a bundle
    // exactly as the runtime does, with no sockets in scope.
    const bundle = new Bundle(dag);
    expect(bundle.getTaskHandler("bundle_api_dag", "extract")).toBe(handler);
    bundle.register(new Dag("late_dag"));
    expect(bundle.getTaskHandler("late_dag", "extract")).toBeUndefined();
  });

  it("keeps bundle enumeration out of the public surface", () => {
    const bundle = new Bundle();
    for (const name of ["listTasks", "listDags"]) {
      expect(name in bundle).toBe(false);
    }
    expectTypeOf<keyof Bundle>().toEqualTypeOf<"register" | "serve" | "getTaskHandler">();
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

  it("exports TaskHandler as the mixed-language authoring surface", () => {
    const transform = async () => "transformed";
    const bundle = new Bundle(new TaskHandler("py_etl", "transform", transform));

    expect(bundle.getTaskHandler("py_etl", "transform")).toBe(transform);
    // Identity and a body, and no more: no schedule, no task order, no dag_id
    // of its own to declare.
    expectTypeOf<keyof TaskHandler>().toEqualTypeOf<"dagId" | "taskId">();
    expectTypeOf<TaskHandler["dagId"]>().toEqualTypeOf<string>();
    expectTypeOf<TaskHandler["taskId"]>().toEqualTypeOf<string>();

    // The handler's own parameter type is inferred, so a typed handler needs
    // no type argument written out at the registration site.
    const typed = new TaskHandler(
      "py_etl",
      "report",
      async ({ regionCode }: { regionCode: string }) => regionCode.toUpperCase(),
    );
    expectTypeOf(typed).toEqualTypeOf<TaskHandler<{ regionCode: string }, string>>();
  });

  it("does not let a task handler be wired the way a native task is", () => {
    // The guarantee an earlier draft's separate MixedLangDag class existed to
    // provide: a handler has no factory to call, so calling one is a compile
    // error rather than a runtime throw.
    const rejectsFactoryMisuse = () => {
      const handler = new TaskHandler("py_etl", "transform", async () => undefined);
      // @ts-expect-error a task handler is a value, not a callable task factory.
      handler();
      // @ts-expect-error dagId and taskId are positional, not an options object.
      new TaskHandler({ dagId: "py_etl", taskId: "transform" }, async () => undefined);
      // @ts-expect-error the task_id is always written out, never derived.
      new TaskHandler("py_etl", async () => undefined);
      // @ts-expect-error a handler does not expose the function it carries.
      void handler.handler;
    };
    void rejectsFactoryMisuse;
  });

  it("exports withArgNames for a name the Python side never used", () => {
    interface ReportArgs {
      label: string;
      threshold: number;
    }
    const report = withArgNames({ label: "run_label" }, async ({ label }: ReportArgs) => label);

    // Wrapping keeps the handler's own type, so the result registers like any
    // other handler and nothing at the registration site has to change.
    expectTypeOf(report).toEqualTypeOf<TaskFunction<ReportArgs, string>>();
    expect(
      new Bundle(new TaskHandler("etl", "report", report)).getTaskHandler("etl", "report"),
    ).toBe(report);
    expectTypeOf<ArgNameMap<ReportArgs>>().toEqualTypeOf<{
      readonly label?: string;
      readonly threshold?: string;
    }>();

    const rejectsUnknownKeys = () => {
      // @ts-expect-error "labl" is not a parameter of ReportArgs; "label" is.
      withArgNames({ labl: "run_label" }, async ({ label }: ReportArgs) => label);
    };
    void rejectsUnknownKeys;
    // Reading the renames back is the runtime's business, not an author's.
    expectTypeOf<typeof sdk>().not.toHaveProperty("getArgNames");
    expect("getArgNames" in sdk).toBe(false);
  });

  describe("the task-handler getters", () => {
    it("throw outside a handler, naming the accessor", () => {
      // The full scope behaviour is covered in tests/sdk/task-scope.test.ts;
      // this pins that both reach the package root and say what went wrong.
      expect(() => getContext()).toThrow(/^getContext\(\) is only available inside a task handler/);
      expect(() => getClient()).toThrow(/^getClient\(\) is only available inside a task handler/);
    });

    it("are the only way a handler reaches the runtime", () => {
      // A handler is a plain function of its own data: the parameter carries
      // the Dag's arguments and nothing else, and the scope is not something
      // an author installs.
      expectTypeOf<TaskFunction>().toEqualTypeOf<(args: void) => unknown | Promise<unknown>>();
      expectTypeOf<TaskFunction<{ regionCode: string }, number>>().toEqualTypeOf<
        (args: { regionCode: string }) => number | Promise<number>
      >();
      expectTypeOf<typeof getContext>().toEqualTypeOf<() => TaskContext>();
      expectTypeOf<typeof getClient>().toEqualTypeOf<() => TaskClient>();
      for (const name of ["TaskHandlerArgs", "runInTaskScope", "TaskScope"]) {
        expect(name in sdk).toBe(false);
      }
      expectTypeOf<typeof sdk>().not.toHaveProperty("runInTaskScope");
    });
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

  it("reaches the runtime only through bundle.serve(), which takes nothing", () => {
    // One verb in and one verb out: `serveDags` is gone, and the coordinator
    // stays unnamed because the object that holds the Dags serves them itself.
    expectTypeOf<Bundle["serve"]>().toEqualTypeOf<() => Promise<void>>();
    expectTypeOf<Bundle["register"]>().toEqualTypeOf<(...items: Registerable[]) => void>();
    expectTypeOf<ConstructorParameters<typeof Bundle>>().toEqualTypeOf<Registerable[]>();
    expectTypeOf<Registerable>().toEqualTypeOf<Dag | TaskHandler<never, unknown>>();
    for (const name of ["serveDags", "DagRegistry"]) {
      expect(name in sdk).toBe(false);
    }
    expectTypeOf<typeof sdk>().not.toHaveProperty("serveDags");
    expectTypeOf<typeof sdk>().not.toHaveProperty("DagRegistry");
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
      <TArgs = void, TReturn = unknown>(
        taskId: string,
        handler: TaskFunction<TArgs, TReturn>,
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
      // @ts-expect-error a bundle is built from Dags, not from task handles.
      new Bundle(upstream);
      // @ts-expect-error serve() takes nothing; the bundle already holds it all.
      new Bundle(dag).serve(dag);
    };
    void rejectsPositionalMisuse;
    // @ts-expect-error the TaskRef handle is opaque and does not expose the handler.
    expectTypeOf<TaskRef>().toHaveProperty("handler");
    // @ts-expect-error XCom values must be JSON-compatible.
    acceptsSetXComOpts({ key: "result", value: new Date() });
  });
});
