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

/* eslint-disable unicorn/no-null */
import type { TFunction } from "i18next";
import { describe, expect, it } from "vitest";

import type { TaskInstancesLogResponse } from "openapi/requests/types.gen";
import {
  extractTIContext,
  renderStructuredLog,
  renderTIContextPreamble,
} from "src/components/renderStructuredLog";
import { parseStreamingLogContent } from "src/utils/logs";

/** Same construction as Logs.tsx getLogString (download path). */
const logStringForDownload = (
  fetchedData: TaskInstancesLogResponse | undefined,
  logLevelFilters: Array<string>,
  translate: TFunction,
) => {
  const lines = parseStreamingLogContent(fetchedData);
  const tiContext = extractTIContext(lines);

  const rendered = lines.map((line) =>
    renderStructuredLog({
      index: 0,
      logLevelFilters,
      logLink: "",
      logMessage: line,
      renderingMode: "text",
      showSource: false,
      showTimestamp: true,
      sourceFilters: [],
      translate,
    }),
  );

  if (tiContext !== undefined) {
    const firstEndGroup = lines.findIndex((line) => {
      const text = typeof line === "string" ? line : line.event;

      return text.includes("::endgroup::");
    });

    rendered.splice(
      firstEndGroup === -1 ? 0 : firstEndGroup + 1,
      0,
      renderTIContextPreamble(tiContext, "text", "Task Identity") as string,
    );
  }

  return rendered.filter((line) => line !== "").join("\n");
};

const tiLine = (event: string, timestamp: string) => ({
  dag_id: "my_dag",
  event,
  level: "info",
  map_index: -1 as const,
  run_id: "run_1",
  task_id: "my_task",
  ti_id: "abc-123",
  timestamp,
  try_number: 1,
});

describe("Task log download content (TI context)", () => {
  const translate = ((key: string) => key) as unknown as TFunction;

  it("injects Task Identity preamble after the source details endgroup", () => {
    const fetchedData: TaskInstancesLogResponse = {
      content: [
        { event: "::group::Log message source details", sources: ["/logs/a.log", "/logs/b.log"] },
        { event: "some source detail" },
        { event: "::endgroup::" },
        tiLine("First log line", "2026-01-01T00:00:00Z"),
        tiLine("Second log line", "2026-01-01T00:00:01Z"),
      ],
      continuation_token: null,
    };

    const text = logStringForDownload(fetchedData, [], translate);
    const lines = text.split("\n");
    const preambleIdx = lines.findIndex((line) => line.includes("Task Identity"));
    const endGroupIdx = lines.findIndex((line) => line.includes("::endgroup::"));
    const firstLogIdx = lines.findIndex((line) => line.includes("First log line"));

    expect(preambleIdx).toBeGreaterThan(endGroupIdx);
    expect(preambleIdx).toBeLessThan(firstLogIdx);
  });

  it("does not include TI context fields on individual log lines", () => {
    const fetchedData: TaskInstancesLogResponse = {
      content: [
        { event: "::group::Log message source details", sources: ["/logs/a.log"] },
        { event: "::endgroup::" },
        tiLine("Task started", "2026-01-01T00:00:00Z"),
      ],
      continuation_token: null,
    };

    const text = logStringForDownload(fetchedData, [], translate);
    const taskStartedLine = text.split("\n").find((line) => line.includes("Task started"));

    expect(taskStartedLine).toBeDefined();
    expect(taskStartedLine).not.toContain("ti_id=");
    expect(taskStartedLine).not.toContain("dag_id=");
    expect(taskStartedLine).not.toContain("run_id=");
  });

  it("omits the preamble when no TI context fields are present", () => {
    const fetchedData: TaskInstancesLogResponse = {
      content: [
        { event: "::group::Log message source details", sources: ["/logs/a.log"] },
        { event: "::endgroup::" },
        { event: "plain log line", level: "info", timestamp: "2026-01-01T00:00:00Z" },
      ],
      continuation_token: null,
    };

    const text = logStringForDownload(fetchedData, [], translate);

    expect(text).not.toContain("Task Identity");
  });
});

describe("Task log download content (log level filter)", () => {
  const translate = ((key: string) => key) as unknown as TFunction;

  it("is empty when every structured line is excluded by the level filter", () => {
    const fetchedData: TaskInstancesLogResponse = {
      content: [
        {
          event: "hello",
          level: "info",
          logger: "task.stdout",
          timestamp: "2025-09-11T17:44:52.597476Z",
        },
      ],
      continuation_token: null,
    };

    const text = logStringForDownload(fetchedData, ["error"], translate);

    expect(text).toBe("");
  });

  it("is empty when structured lines have no level and any log level filter is set", () => {
    const fetchedData: TaskInstancesLogResponse = {
      content: [
        {
          event: "[timestamp] {file.py:1} INFO - legacy line without level field",
          timestamp: "2025-02-28T10:49:09.679000+05:30",
        },
      ],
      continuation_token: null,
    };

    const text = logStringForDownload(fetchedData, ["info"], translate);

    expect(text).toBe("");
  });

  it("does not prefix the download with newlines when earlier lines are filtered out", () => {
    const fetchedData: TaskInstancesLogResponse = {
      content: [
        {
          event: "hidden-group-marker",
          level: "debug",
          logger: "task.stdout",
          timestamp: "2025-09-11T17:44:52.597476Z",
        },
        {
          event: "visible-line",
          level: "info",
          logger: "task.stdout",
          timestamp: "2025-09-11T17:44:52.597500Z",
        },
      ],
      continuation_token: null,
    };

    const text = logStringForDownload(fetchedData, ["info"], translate);

    expect(text.startsWith("\n")).toBe(false);
    expect(text).toContain("visible-line");
    expect(text).not.toContain("hidden-group-marker");
  });

  it("includes matching structured lines when the filter matches level", () => {
    const fetchedData: TaskInstancesLogResponse = {
      content: [
        {
          event: "hello",
          level: "info",
          logger: "task.stdout",
          timestamp: "2025-09-11T17:44:52.597476Z",
        },
      ],
      continuation_token: null,
    };

    const text = logStringForDownload(fetchedData, ["info"], translate);

    expect(text.length).toBeGreaterThan(0);
    expect(text).toContain("hello");
    expect(text).toContain("INFO");
  });
});
