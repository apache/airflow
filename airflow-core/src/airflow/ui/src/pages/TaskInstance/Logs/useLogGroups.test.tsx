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
import { renderHook } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import type { ParsedLogEntry } from "src/queries/useLogs";

import { useLogGroups } from "./useLogGroups";

const parsedLogs: Array<ParsedLogEntry> = [
  { element: "task identity preamble" },
  { element: "::group::setup", group: { id: 0, level: 0, type: "header" } },
  { element: "line 1", group: { id: 0, level: 0, type: "line" }, lineNumber: 1 },
  { element: "line 2", group: { id: 0, level: 0, type: "line" }, lineNumber: 2 },
  { element: "line 3", lineNumber: 3 },
];

describe("useLogGroups", () => {
  it("maps line numbers to visible indexes when groups are expanded", () => {
    const { result } = renderHook(() => useLogGroups({ expanded: true, parsedLogs }));

    expect([...result.current.lineNumberToVisibleIndex]).toStrictEqual([
      [1, 2],
      [2, 3],
      [3, 4],
    ]);
  });

  it("skips lines hidden inside a collapsed group", () => {
    const { result } = renderHook(() => useLogGroups({ expanded: false, parsedLogs }));

    expect([...result.current.lineNumberToVisibleIndex]).toStrictEqual([[3, 2]]);
  });
});
