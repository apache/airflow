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
import { serializeDag } from "../../src/coordinator/serde.js";
import { Bundle, finalizeBundleDags, getBundleTask } from "../../src/sdk/bundle.js";
import { Dag, type TaskRef } from "../../src/sdk/dag.js";
import {
  approval,
  humanInput,
  type HumanInputResult,
  type HumanInputSpec,
} from "../../src/sdk/human-input.js";
import { decodeDatetime } from "../../src/sdk/human-input-execute.js";

type Json = Record<string, unknown>;

function serializedTasks(dag: Dag) {
  const tasks = (serializeDag(dag, "", ".") as Json)["tasks"] as { __var: Json }[];
  return new Map(tasks.map(({ __var }) => [__var["task_id"] as string, __var]));
}

describe("humanInput", () => {
  it("applies the operator's defaults", () => {
    const task = humanInput({ subject: "Pick", options: ["a", "b"] });

    expect(task).toMatchObject({
      kind: "choice",
      subject: "Pick",
      body: undefined,
      options: ["a", "b"],
      defaults: undefined,
      multiple: false,
      assignees: [],
      responseTimeout: undefined,
      onReject: undefined,
    });
    expect(Object.isFrozen(task)).toBe(true);
  });

  it("copies the options, so editing the array passed in changes nothing", () => {
    const options = ["a", "b"];
    const task = humanInput({ subject: "Pick", options });
    options.push("c");

    expect(task.options).toEqual(["a", "b"]);
  });

  it("allows several defaults when several options may be chosen", () => {
    const task = humanInput({
      subject: "s",
      options: ["a", "b"],
      defaults: ["a", "b"],
      multiple: true,
    });

    expect(task.defaults).toEqual(["a", "b"]);
  });

  describe("rejects", () => {
    it.each<[string, unknown, RegExp]>([
      [
        "no options",
        { subject: "s", options: [] },
        /humanInput\(\.\.\.\) needs "options": a non-empty array of strings/,
      ],
      [
        "a duplicate option",
        { subject: "s", options: ["a", "a"] },
        /option "options" lists "a" twice/,
      ],
      [
        "an empty option",
        { subject: "s", options: [""] },
        /option "options" holds a value that is not a non-empty string/,
      ],
      [
        "a default it does not offer",
        { subject: "s", options: ["a"], defaults: ["b"] },
        /option "defaults" holds "b", which is not one of the options \["a"\]/,
      ],
      [
        "two defaults for a single choice",
        { subject: "s", options: ["a", "b"], defaults: ["a", "b"] },
        /humanInput\(\.\.\.\) gives 2 defaults, but "multiple" is not set/,
      ],
      [
        "no subject",
        { options: ["a"] },
        /humanInput\(\.\.\.\) needs a "subject": a string, or a function returning one/,
      ],
      ["an empty subject", { subject: "", options: ["a"] }, /option "subject" cannot be empty/],
      [
        "a numeric body",
        { subject: "s", body: 3, options: ["a"] },
        /option "body" must be a string, or a function returning one/,
      ],
      [
        "a zero timeout",
        { subject: "s", options: ["a"], responseTimeout: 0 },
        /"responseTimeout" must be a positive whole number of seconds/,
      ],
      [
        "a fractional timeout",
        { subject: "s", options: ["a"], responseTimeout: 1.5 },
        /"responseTimeout" must be a positive whole number of seconds/,
      ],
      [
        "an assignee with no id",
        { subject: "s", options: ["a"], assignees: [{ id: "", name: "Ada" }] },
        /option "assignees" holds \{"id":"","name":"Ada"\}; each assignee is \{ id, name \}/,
      ],
      [
        "an option it does not declare",
        { subject: "s", options: ["a"], onReject: "skip" },
        /Unknown option "onReject" for humanInput\(\.\.\.\)/,
      ],
    ])("%s", (_label, spec, error) => {
      expect(() => humanInput(spec as HumanInputSpec)).toThrowError(error);
    });
  });
});

describe("approval", () => {
  it("offers exactly Approve and Reject, and skips on Reject by default", () => {
    expect(approval({ subject: "Ship?" })).toMatchObject({
      kind: "approval",
      options: ["Approve", "Reject"],
      multiple: false,
      onReject: "skip",
    });
  });

  it("takes one default, the answer given on timeout", () => {
    expect(approval({ subject: "Ship?", defaults: "Reject" }).defaults).toEqual(["Reject"]);
  });

  describe("rejects", () => {
    it.each<[string, unknown, RegExp]>([
      [
        "options",
        { subject: "s", options: ["a"] },
        /Unknown option "options" for approval\(\.\.\.\)/,
      ],
      [
        "multiple",
        { subject: "s", multiple: true },
        /Unknown option "multiple" for approval\(\.\.\.\)/,
      ],
      [
        "a default it does not offer",
        { subject: "s", defaults: "Maybe" },
        /option "defaults" must be "Approve" or "Reject"/,
      ],
      [
        "an unknown onReject",
        { subject: "s", onReject: "ignore" },
        /option "onReject" holds "ignore"; use one of "skip", "skipAll", "fail"/,
      ],
    ])("%s", (_label, spec, error) => {
      expect(() => approval(spec as never)).toThrowError(error);
    });
  });
});

describe("a human-input task in a Dag", () => {
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
    dag.task("choose", humanInput({ subject: "s", options: ["a"] }))();

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

  it("types its result as the response, and its inputs from the text function", () => {
    const dag = new Dag("d");
    const factory = dag.task(
      "decide",
      approval({ subject: ({ version }: { version: string }) => version }),
    );

    expectTypeOf(factory).parameter(0).toEqualTypeOf<{ version: TaskRef | string }>();
    expectTypeOf(factory).returns.toEqualTypeOf<TaskRef<HumanInputResult>>();
  });

  describe("rejects", () => {
    it("no taskId", () => {
      const dag = new Dag("d");

      expect(() => dag.task(approval({ subject: "s" }), {})).toThrowError(
        /A human-input task of Dag "d" has no taskId/,
      );
    });
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
