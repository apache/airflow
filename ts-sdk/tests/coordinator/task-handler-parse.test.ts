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

function declare(bundle: Bundle) {
  return declareTaskHandlers(bundle, { file: FILE });
}

describe("declareTaskHandlers", () => {
  it("declares every Dag's handlers in registration order", () => {
    const bundle = new Bundle(
      new TaskHandler("etl", "extract", noop),
      new TaskHandler("reports", "send", noop),
      new TaskHandler("etl", "load", noop),
    );

    const result = declare(bundle);

    expect(result).toStrictEqual({
      type: "TaskHandlerParsingResult",
      fileloc: FILE,
      task_handlers: {
        etl: [
          { task_id: "extract", binding: "named", params: null },
          { task_id: "load", binding: "named", params: null },
        ],
        reports: [{ task_id: "send", binding: "named", params: null }],
      },
    });
    expect(Object.keys(result.task_handlers)).toEqual(["etl", "reports"]);
  });

  it("lists no params for a handler with withArgNames renames", () => {
    interface ReportArgs {
      label: string;
      owner: string;
    }
    const report = withArgNames(
      { label: "run_label", owner: "team" },
      async ({ label }: ReportArgs) => label,
    );

    const result = declare(new Bundle(new TaskHandler("etl", "report", report)));

    expect(result.task_handlers).toStrictEqual({
      etl: [{ task_id: "report", binding: "named", params: null }],
    });
  });

  it("does not declare a Dag declared in TypeScript", () => {
    const native = new Dag("native");
    native.task("run", noop)();

    const mixed = new Bundle(native, new TaskHandler("etl", "extract", noop));

    expect(declare(new Bundle(native)).task_handlers).toEqual({});
    expect(Object.keys(declare(mixed).task_handlers)).toEqual(["etl"]);
  });

  it("keeps a Dag named __proto__ as a key", () => {
    const result = declare(new Bundle(new TaskHandler("__proto__", "run", noop)));

    expect(Object.keys(result.task_handlers)).toEqual(["__proto__"]);
    expect(Object.getPrototypeOf(result.task_handlers)).toBe(Object.prototype);
  });
});
