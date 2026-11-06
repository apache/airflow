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

// What a HITL task's factory is called with, which is a compile-time property: nothing here
// checks it at run time.

import { describe, expect, expectTypeOf, it } from "vitest";
import { approval, hitl, type HITLResult } from "../../src/hitl/index.js";
import { Dag, type TaskRef } from "../../src/sdk/dag.js";

describe("the inputs of a HITL task", () => {
  it("are those of the subject and the body together when they read different ones", () => {
    const dag = new Dag("d");
    const factory = dag.task(
      "decide",
      hitl({
        subject: ({ version }: { version: string }) => `Ship ${version}?`,
        body: ({ notes }: { notes: string }) => notes,
        options: ["yes", "no"],
      }),
    );

    expectTypeOf(factory).parameter(0).toEqualTypeOf<{
      version: TaskRef | string;
      notes: TaskRef | string;
    }>();
    expectTypeOf(factory).returns.toEqualTypeOf<TaskRef<HITLResult>>();
    expectTypeOf(factory).toBeCallableWith({ version: "1.4", notes: "Faster" });

    const rejectsWrongInputs = () => {
      // @ts-expect-error -- `notes` is read by the body, so it is required.
      factory({ version: "1.4" });
      // @ts-expect-error -- `note` is not an input of the task.
      factory({ version: "1.4", note: "Faster" });
    };
    expect(rejectsWrongInputs).toBeTypeOf("function");
  });

  it("are the subject's, for the body too, when only the subject reads them", () => {
    const dag = new Dag("d");
    const factory = dag.task(
      "decide",
      approval({ subject: ({ version }: { version: string }) => `Ship ${version}?` }),
    );

    expectTypeOf(factory).parameter(0).toEqualTypeOf<{ version: TaskRef | string }>();
    expectTypeOf(factory).returns.toEqualTypeOf<TaskRef<HITLResult>>();
  });

  it("are the body's when only the body reads them", () => {
    const dag = new Dag("d");
    const factory = dag.task(
      "decide",
      approval({ subject: "Ship it?", body: ({ notes }: { notes: string }) => notes }),
    );

    expectTypeOf(factory).parameter(0).toEqualTypeOf<{ notes: TaskRef | string }>();
  });

  it("are given by an explicit type argument for a fixed subject", () => {
    const dag = new Dag("d");
    const factory = dag.task("x", approval<{ a: string }>({ subject: "s" }));

    expectTypeOf(factory).parameter(0).toEqualTypeOf<{ a: TaskRef | string }>();
    expectTypeOf(factory).toBeCallableWith({ a: "value" });

    const rejectsWrongInputs = () => {
      // @ts-expect-error -- `b` is not an input of the task.
      factory({ b: "value" });
      // @ts-expect-error -- the task takes inputs.
      factory();
    };
    expect(rejectsWrongInputs).toBeTypeOf("function");
  });

  it("are given by an explicit type argument for each text of a hitl", () => {
    const dag = new Dag("d");
    const factory = dag.task(
      "x",
      hitl<{ a: string }, { b: number }>({ subject: "s", options: ["o"] }),
    );

    expectTypeOf(factory)
      .parameter(0)
      .toEqualTypeOf<{ a: TaskRef | string; b: TaskRef | number }>();
  });

  it("are none for a fixed subject and body", () => {
    const dag = new Dag("d");
    const factory = dag.task("x", approval({ subject: "s" }));

    expectTypeOf(factory).toEqualTypeOf<() => TaskRef<HITLResult>>();
    expectTypeOf(factory).toBeCallableWith();

    const rejectsInputs = () => {
      // @ts-expect-error -- the task takes no inputs.
      factory({ a: "value" });
    };
    expect(rejectsInputs).toBeTypeOf("function");
  });
});
