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
import type { TFunction } from "i18next";
import { describe, expect, it } from "vitest";

import { getDownloadText, getHighlightColor, splitBySearchQuery } from "./utils";

const translate = ((key: string) => key) as unknown as TFunction;

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

describe("getDownloadText", () => {
  const baseOptions = {
    logLevelFilters: [],
    showSource: false,
    showTimestamp: false,
    sourceFilters: [],
    translate,
  };

  it("places Task Identity preamble after the source details endgroup, before the first log line", () => {
    const fetchedData = {
      content: [
        { event: "::group::Log message source details", sources: ["/logs/a.log", "/logs/b.log"] },
        { event: "some source detail" },
        { event: "::endgroup::" },
        tiLine("First log line", "2026-01-01T00:00:00Z"),
        tiLine("Second log line", "2026-01-01T00:00:01Z"),
      ],
      continuation_token: null,
    };

    const lines = getDownloadText({ ...baseOptions, fetchedData });
    const preambleIdx = lines.findIndex((line) => line.includes("Task Identity"));
    const endGroupIdx = lines.findIndex((line) => line.includes("::endgroup::"));
    const firstLogIdx = lines.findIndex((line) => line.includes("First log line"));

    expect(preambleIdx).toBeGreaterThan(endGroupIdx);
    expect(preambleIdx).toBeLessThan(firstLogIdx);
  });

  it("does not include TI context fields on individual log lines", () => {
    const fetchedData = {
      content: [
        { event: "::group::Log message source details", sources: ["/logs/a.log"] },
        { event: "::endgroup::" },
        tiLine("Task started", "2026-01-01T00:00:00Z"),
      ],
      continuation_token: null,
    };

    const lines = getDownloadText({ ...baseOptions, fetchedData });
    const taskStartedLine = lines.find((line) => line.includes("Task started"));

    expect(taskStartedLine).toBeDefined();
    expect(taskStartedLine).not.toContain("ti_id=");
    expect(taskStartedLine).not.toContain("dag_id=");
    expect(taskStartedLine).not.toContain("run_id=");
  });

  it("omits the preamble when no TI context fields are present", () => {
    const fetchedData = {
      content: [
        { event: "::group::Log message source details", sources: ["/logs/a.log"] },
        { event: "::endgroup::" },
        { event: "plain log line", level: "info", timestamp: "2026-01-01T00:00:00Z" },
      ],
      continuation_token: null,
    };

    const lines = getDownloadText({ ...baseOptions, fetchedData });

    expect(lines.every((line) => !line.includes("Task Identity"))).toBe(true);
  });
});

describe("getHighlightColor", () => {
  it("returns yellow.emphasized for the current search match", () => {
    expect(
      getHighlightColor({
        currentMatchLineIndex: 3,
        hash: "",
        index: 3,
        searchMatchIndices: new Set([1, 3, 5]),
      }),
    ).toBe("yellow.emphasized");
  });

  it("returns yellow.subtle for a non-current search match", () => {
    expect(
      getHighlightColor({
        currentMatchLineIndex: 1,
        hash: "",
        index: 3,
        searchMatchIndices: new Set([1, 3, 5]),
      }),
    ).toBe("yellow.subtle");
  });

  it("returns brand.emphasized for the URL-hash-linked line when no search is active", () => {
    expect(
      getHighlightColor({
        hash: "5",
        index: 4, // hash "5" maps to index 4 (1-based to 0-based)
        searchMatchIndices: undefined,
      }),
    ).toBe("brand.emphasized");
  });

  it("returns transparent when no condition matches", () => {
    expect(
      getHighlightColor({
        hash: "",
        index: 2,
        searchMatchIndices: undefined,
      }),
    ).toBe("transparent");
  });

  it("returns transparent when search is active but line is not a match", () => {
    expect(
      getHighlightColor({
        currentMatchLineIndex: 0,
        hash: "",
        index: 7,
        searchMatchIndices: new Set([0, 2]),
      }),
    ).toBe("transparent");
  });

  it("current match takes priority over hash highlight on same line", () => {
    expect(
      getHighlightColor({
        currentMatchLineIndex: 4,
        hash: "5",
        index: 4,
        searchMatchIndices: new Set([4]),
      }),
    ).toBe("yellow.emphasized");
  });

  it("search match highlight takes priority over hash highlight on same line", () => {
    expect(
      getHighlightColor({
        currentMatchLineIndex: 0,
        hash: "5",
        index: 4,
        searchMatchIndices: new Set([0, 4]),
      }),
    ).toBe("yellow.subtle");
  });

  it("returns transparent when searchMatchIndices is an empty Set", () => {
    expect(
      getHighlightColor({
        currentMatchLineIndex: undefined,
        hash: "",
        index: 0,
        searchMatchIndices: new Set(),
      }),
    ).toBe("transparent");
  });
});

describe("splitBySearchQuery", () => {
  it("returns the full text as a single non-highlight segment when query is empty", () => {
    expect(splitBySearchQuery("hello world", "")).toEqual([{ highlight: false, text: "hello world" }]);
  });

  it("returns the full text when query does not match", () => {
    expect(splitBySearchQuery("hello world", "xyz")).toEqual([{ highlight: false, text: "hello world" }]);
  });

  it("highlights a single match", () => {
    expect(splitBySearchQuery("hello world", "world")).toEqual([
      { highlight: false, text: "hello " },
      { highlight: true, text: "world" },
    ]);
  });

  it("highlights multiple matches", () => {
    expect(splitBySearchQuery("foo bar foo", "foo")).toEqual([
      { highlight: true, text: "foo" },
      { highlight: false, text: " bar " },
      { highlight: true, text: "foo" },
    ]);
  });

  it("is case-insensitive", () => {
    expect(splitBySearchQuery("Hello HELLO hello", "hello")).toEqual([
      { highlight: true, text: "Hello" },
      { highlight: false, text: " " },
      { highlight: true, text: "HELLO" },
      { highlight: false, text: " " },
      { highlight: true, text: "hello" },
    ]);
  });

  it("handles match at the start", () => {
    expect(splitBySearchQuery("error: something", "error")).toEqual([
      { highlight: true, text: "error" },
      { highlight: false, text: ": something" },
    ]);
  });

  it("handles match at the end", () => {
    expect(splitBySearchQuery("something error", "error")).toEqual([
      { highlight: false, text: "something " },
      { highlight: true, text: "error" },
    ]);
  });

  it("handles entire string as match", () => {
    expect(splitBySearchQuery("error", "error")).toEqual([{ highlight: true, text: "error" }]);
  });
});
