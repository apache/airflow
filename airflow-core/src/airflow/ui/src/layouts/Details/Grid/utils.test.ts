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

import { type GridTask, estimateTaskNameColumnWidthPx } from "./utils";

const baseNode = {
  id: "t1",
  is_mapped: false,
  label: "task_a",
} as const;

describe("estimateTaskNameColumnWidthPx", () => {
  it("returns the layout minimum when there are no nodes", () => {
    expect(estimateTaskNameColumnWidthPx([])).toBe(200);
  });

  it("returns at least the layout minimum for a short label", () => {
    const nodes: Array<GridTask> = [{ ...baseNode, depth: 0, label: "a" } as GridTask];

    expect(estimateTaskNameColumnWidthPx(nodes)).toBe(200);
  });

  it("increases width for longer labels, depth, group chevron, and mapped hint", () => {
    const plain: Array<GridTask> = [{ ...baseNode, depth: 0, id: "a", label: "x".repeat(40) } as GridTask];
    const deep: Array<GridTask> = [{ ...baseNode, depth: 4, id: "b", label: "x".repeat(40) } as GridTask];
    const group: Array<GridTask> = [
      { ...baseNode, depth: 0, id: "c", isGroup: true, label: "x".repeat(40) } as GridTask,
    ];
    const mapped: Array<GridTask> = [
      { ...baseNode, depth: 0, id: "d", is_mapped: true, label: "x".repeat(40) } as GridTask,
    ];

    const wPlain = estimateTaskNameColumnWidthPx(plain);
    const wDeep = estimateTaskNameColumnWidthPx(deep);
    const wGroup = estimateTaskNameColumnWidthPx(group);
    const wMapped = estimateTaskNameColumnWidthPx(mapped);

    expect(wDeep).toBeGreaterThan(wPlain);
    expect(wGroup).toBeGreaterThan(wPlain);
    expect(wMapped).toBeGreaterThan(wPlain);
  });
});
