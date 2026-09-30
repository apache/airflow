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

import { declareTaskHandlers } from "../../src/coordinator/task-handler-parse.js";
import { withArgNames } from "../../src/sdk/arg-names.js";
import { Bundle } from "../../src/sdk/bundle.js";
import { Dag } from "../../src/sdk/dag.js";
import { TaskHandler } from "../../src/sdk/task-handler.js";

const FILE = "/bundles/etl.min.mjs";

async function noop(): Promise<void> {}

function declare(bundle: Bundle, dagIds: unknown) {
  return declareTaskHandlers(bundle, { file: FILE, dag_ids: dagIds as string[] });
}

describe("declareTaskHandlers", () => {
  it("declares each requested Dag's handlers in registration order", () => {
    const bundle = new Bundle(
      new TaskHandler("etl", "extract", noop),
      new TaskHandler("reports", "send", noop),
      new TaskHandler("etl", "load", noop),
    );

    const result = declare(bundle, ["reports", "etl"]);

    expect(result).toStrictEqual({
      type: "TaskHandlerParsingResult",
      fileloc: FILE,
      task_handlers: {
        etl: [
          { task_id: "extract", binding: "named_open", params: [] },
          { task_id: "load", binding: "named_open", params: [] },
        ],
        reports: [{ task_id: "send", binding: "named_open", params: [] }],
      },
    });
    expect(Object.keys(result.task_handlers)).toEqual(["etl", "reports"]);
  });

  it("declares each distinct withArgNames target as an exact, optional param", () => {
    interface ReportArgs {
      label: string;
      title: string;
      owner: string;
    }
    const report = withArgNames(
      { label: "run_label", title: "run_label", owner: "team" },
      async ({ label }: ReportArgs) => label,
    );

    const result = declare(new Bundle(new TaskHandler("etl", "report", report)), ["etl"]);

    expect(result.task_handlers).toEqual({
      etl: [
        {
          task_id: "report",
          binding: "named_open",
          params: [
            { name: "run_label", value_schema: null, required: false, exact_name: true },
            { name: "team", value_schema: null, required: false, exact_name: true },
          ],
        },
      ],
    });
  });

  it("leaves out unrequested Dags and requested Dags with no handler", () => {
    const bundle = new Bundle(
      new TaskHandler("etl", "extract", noop),
      new TaskHandler("reports", "send", noop),
    );

    expect(Object.keys(declare(bundle, ["etl", "missing"]).task_handlers)).toEqual(["etl"]);
    expect(declare(bundle, ["missing"]).task_handlers).toEqual({});
  });

  it("does not declare a Dag declared in TypeScript", () => {
    const native = new Dag("native");
    native.task("run", noop)();
    const bundle = new Bundle(native, new TaskHandler("etl", "extract", noop));

    expect(Object.keys(declare(bundle, ["native", "etl"]).task_handlers)).toEqual(["etl"]);
  });

  it("keeps a Dag named __proto__ as a key", () => {
    const result = declare(new Bundle(new TaskHandler("__proto__", "run", noop)), ["__proto__"]);

    expect(Object.keys(result.task_handlers)).toEqual(["__proto__"]);
    expect(Object.getPrototypeOf(result.task_handlers)).toBe(Object.prototype);
  });

  it.each([[undefined], ["etl"], [["etl", 1]]])("rejects dag_ids of %j", (dagIds) => {
    const bundle = new Bundle(new TaskHandler("etl", "extract", noop));

    expect(() => declare(bundle, dagIds)).toThrow(/dag_ids must be a list of strings/);
  });
});
