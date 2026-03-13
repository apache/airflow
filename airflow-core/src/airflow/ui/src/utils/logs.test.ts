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

import { parseStreamingLogContent } from "./logs";

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
    // eslint-disable-next-line unicorn/no-null
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
