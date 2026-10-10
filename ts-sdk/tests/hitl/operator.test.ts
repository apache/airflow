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

import { describe, expect, it } from "vitest";
import { serializeDag } from "../../src/coordinator/serde.js";
import { approval, hitl } from "../../src/hitl/index.js";
import { decodeDatetime } from "../../src/hitl/operator.js";
import { Bundle, finalizeBundleDags, getBundleTask } from "../../src/sdk/bundle.js";
import { Dag } from "../../src/sdk/dag.js";

type Json = Record<string, unknown>;

function serializedTasks(dag: Dag) {
  const tasks = (serializeDag(dag, "", ".") as Json)["tasks"] as { __var: Json }[];
  return new Map(tasks.map(({ __var }) => [__var["task_id"] as string, __var]));
}

describe("a HITL task in a Dag", () => {
  it("is placed like any task, takes inputs, and runs without a handler", () => {
    const dag = new Dag("d");
    const build = dag.task("build", async () => ({ version: "1.4" }));
    const decide = dag.task(
      "decide",
      approval({ subject: ({ report }: { report: { version: string } }) => report.version }),
    )({ report: build() });

    expect(decide).toMatchObject({ dagId: "d", taskId: "decide" });
    const bundle = new Bundle(dag);
    expect(bundle.getTaskHandler("d", "decide")).toBeUndefined();
    expect(getBundleTask(bundle, "d", "decide")).toMatchObject({
      kind: "operator",
      dag,
      operator: { kind: "approval", options: ["Approve", "Reject"] },
    });
    expect(() => finalizeBundleDags(bundle)).not.toThrow();
  });

  it("can be declared inside a task group", () => {
    const dag = new Dag("d");
    dag.taskGroup("release").task("sign_off", approval({ subject: "s" }))();

    expect(dag.taskIds).toEqual(["release.sign_off"]);
  });

  it("serializes as the operator it mirrors, with its argument bindings", () => {
    const dag = new Dag("d");
    const build = dag.task("build", async () => 1);
    dag.task(
      "decide",
      approval({ subject: ({ report }: { report: number }) => `${report}` }),
    )({
      report: build(),
    });
    dag.task("choose", hitl({ subject: "s", options: ["a"] }))();

    const tasks = serializedTasks(dag);
    expect(tasks.get("decide")).toMatchObject({
      task_type: "TypeScriptOperator",
      _operator_name: "ApprovalOperator",
      _can_skip_downstream: true,
      _arg_bindings: [{ name: "report", kind: "xcom", task_id: "build" }],
    });
    expect(tasks.get("choose")).toMatchObject({ _operator_name: "HITLOperator" });
    expect(tasks.get("choose")).not.toHaveProperty("_can_skip_downstream");
  });

  describe("rejects", () => {
    it.each([
      ["hitl", () => hitl({ subject: "s", options: ["a"] }), "choose"],
      ["approval", () => approval({ subject: "s" }), "sign_off"],
    ])("no taskId for %s", (factory, build, example) => {
      const dag = new Dag("d");

      expect(() => dag.task(build(), {})).toThrowError(
        new RegExp(
          `A human-in-the-loop task of Dag "d" has no taskId; name it with dag\\.task\\("${example}", ${factory}\\(`,
        ),
      );
    });

    it("an operator from another copy of the package, before it can run", () => {
      // Stands in for an approval() from a second resolved copy: same brand, not built here.
      const foreign = { ...approval({ subject: "s" }) };
      Object.defineProperty(foreign, Symbol.for("airflow.ts-sdk.Operator"), { value: true });
      const dag = new Dag("d");

      expect(() => dag.task("sign_off", foreign)).toThrowError(
        /A human-in-the-loop task of Dag "d" comes from a different copy of apache-airflow-ts-sdk/,
      );
    });

    it.each([
      ["hitl", hitl({ subject: "s", options: ["a"] })],
      ["approval", approval({ subject: "s" })],
    ])("an executionTimeout on %s, naming responseTimeout instead", (_factory, operator) => {
      const dag = new Dag("d");

      expect(() => dag.task("wait", operator, { executionTimeout: 60 })).toThrowError(
        'Task "wait" of Dag "d" is a human-in-the-loop task, which does not enforce ' +
          "executionTimeout; use responseTimeout instead",
      );
      expect(dag.taskIds).toEqual([]);
    });

    it("an executionTimeout inside a task group", () => {
      const dag = new Dag("d");

      expect(() =>
        dag.taskGroup("g").task("wait", approval({ subject: "s" }), { executionTimeout: 60 }),
      ).toThrowError(/does not enforce executionTimeout; use responseTimeout instead/);
    });
  });

  it("takes other task options", () => {
    const dag = new Dag("d");

    expect(() =>
      dag.task("wait", approval({ subject: "s", responseTimeout: 60 }), { retries: 2 }),
    ).not.toThrow();
  });
});

describe("decodeDatetime", () => {
  const seconds = 1791462615.123456;

  it.each<[string, unknown]>([
    [
      "serde's pendulum DateTime",
      {
        __classname__: "pendulum.datetime.DateTime",
        __version__: 2,
        __data__: { timestamp: seconds },
      },
    ],
    [
      "serde's datetime.datetime",
      {
        __classname__: "datetime.datetime",
        __version__: 2,
        __data__: { timestamp: seconds, tz: null },
      },
    ],
  ])("reads %s", (_label, value) => {
    expect(decodeDatetime(value)?.toISOString()).toBe("2026-10-08T12:30:15.123Z");
  });

  it.each([["2026-10-08T12:30:15.123Z"], [1791462615.123], [null], [{ __data__: {} }]])(
    "rejects %j",
    (value) => {
      expect(decodeDatetime(value)).toBeUndefined();
    },
  );
});
