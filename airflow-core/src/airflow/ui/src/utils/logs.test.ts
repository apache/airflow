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
import { describe, it, expect } from "vitest";

import type { TaskInstancesLogResponse } from "openapi/requests/types.gen";

import { isUserCodeFrame, parseStreamingLogContent, resolveSourceFilters } from "./logs";

describe("isUserCodeFrame", () => {
  it.each([
    "/tmp/airflow/dag_bundles/astro/main/dags/non_alert/enrich_ticket.py",
    "/opt/airflow/dags/my_dag.py",
    "/home/user/plugins/my_plugin.py",
    "C:\\airflow\\dags\\my_dag.py",
  ])("treats %s as user code", (filename) => {
    expect(isUserCodeFrame(filename)).toBe(true);
  });

  it.each([
    "/usr/local/lib/python3.14/site-packages/airflow/sdk/execution_time/task_runner.py",
    "/usr/lib/python3/dist-packages/airflow/models/baseoperator.py",
    "C:\\Python314\\Lib\\site-packages\\airflow\\sdk\\bases\\operator.py",
  ])("treats %s as library code", (filename) => {
    expect(isUserCodeFrame(filename)).toBe(false);
  });
});

describe("parseStreamingLogContent", () => {
  it("returns content when data has content property", () => {
    const data: TaskInstancesLogResponse = {
      content: ["log line 1", "log line 2"],
      continuation_token: null,
    };

    expect(parseStreamingLogContent(data)).toEqual(["log line 1", "log line 2"]);
  });

  it("parses string data as newline-separated JSON", () => {
    const data = '"log line 1"\n"log line 2"\n"log line 3"' as unknown as TaskInstancesLogResponse;

    expect(parseStreamingLogContent(data)).toEqual(["log line 1", "log line 2", "log line 3"]);
  });

  it("filters out empty lines when parsing string data", () => {
    const data = '"log line 1"\n\n"log line 2"\n\n\n"log line 3"' as unknown as TaskInstancesLogResponse;

    expect(parseStreamingLogContent(data)).toEqual(["log line 1", "log line 2", "log line 3"]);
  });

  it("returns empty array for undefined data", () => {
    expect(parseStreamingLogContent(undefined)).toEqual([]);
  });

  it("returns empty array for null data", () => {
    const data = null as unknown as TaskInstancesLogResponse;

    expect(parseStreamingLogContent(data)).toEqual([]);
  });

  it("returns object as array when data is an object without content property", () => {
    const data = { someOtherProperty: "value" } as unknown as TaskInstancesLogResponse;

    expect(parseStreamingLogContent(data)).toEqual([{ someOtherProperty: "value" }]);
  });

  it("returns empty array for number data", () => {
    const data = 123 as unknown as TaskInstancesLogResponse;

    expect(parseStreamingLogContent(data)).toEqual([]);
  });

  it("returns empty array when JSON parsing fails", () => {
    const data = "invalid json line\nanother invalid line" as unknown as TaskInstancesLogResponse;

    expect(parseStreamingLogContent(data)).toEqual([]);
  });

  it("handles single log line as string", () => {
    const data = '"single log line"' as unknown as TaskInstancesLogResponse;

    expect(parseStreamingLogContent(data)).toEqual(["single log line"]);
  });

  it("handles empty string", () => {
    const data = "" as unknown as TaskInstancesLogResponse;

    expect(parseStreamingLogContent(data)).toEqual([]);
  });

  it("handles data with empty content array", () => {
    const data: TaskInstancesLogResponse = {
      content: [],
      continuation_token: null,
    };

    expect(parseStreamingLogContent(data)).toEqual([]);
  });

  it("handles single log line object", () => {
    const data = {
      level: "info",
      message: "log message",
      timestamp: "2024-01-01",
    } as unknown as TaskInstancesLogResponse;

    expect(parseStreamingLogContent(data)).toEqual([
      { level: "info", message: "log message", timestamp: "2024-01-01" },
    ]);
  });
});

describe("resolveSourceFilters", () => {
  const available = ["task", "task.stdout"];

  it.each([
    {
      defaults: ["task"],
      expected: ["task.stdout"],
      name: "a selection over the defaults",
      selected: ["task.stdout"],
    },
    { defaults: ["task"], expected: [], name: "all sources over the defaults", selected: ["all"] },
    { defaults: ["task"], expected: ["task"], name: "the defaults without a selection", selected: [] },
    { defaults: ["task", "other"], expected: ["task"], name: "only the defaults the log has", selected: [] },
    {
      defaults: ["other"],
      expected: [],
      name: "no filter when the log has none of the defaults",
      selected: [],
    },
    { defaults: undefined, expected: [], name: "no filter without defaults", selected: undefined },
  ])("uses $name", ({ defaults, expected, selected }) => {
    expect(resolveSourceFilters({ available, defaults, selected })).toEqual(expected);
  });
});
