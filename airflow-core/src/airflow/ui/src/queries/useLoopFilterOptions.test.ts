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

import type { GridNodeResponse } from "openapi/requests/types.gen";

import { loopsForNode, membersOfGroup } from "./useIsLoopGroup";
import { selectActivePass, type LoopOption } from "./useLoopFilterOptions";

const node = (id: string, extra: Partial<GridNodeResponse> = {}): GridNodeResponse =>
  ({ children: null, id, is_loop: false, label: id, ...extra }) as GridNodeResponse;

// A loop declared with prefix_group_id=False does not name its members after itself, so the
// structure is the only thing that says what is inside it.
const STRUCTURE = [
  node("refine", {
    children: [node("improve"), node("evaluate")],
    is_loop: true,
  }),
  node("outer", {
    children: [node("outer.plain"), node("batch", { children: [node("batch.member")], is_loop: true })],
  }),
  node("unrelated"),
];

describe("loopsForNode", () => {
  it("lists every declared loop when no node is named", () => {
    expect(loopsForNode(STRUCTURE)).toEqual(["refine", "batch"]);
  });

  it("finds the loop enclosing a member that is not named after it", () => {
    expect(loopsForNode(STRUCTURE, "improve")).toEqual(["refine"]);
  });

  it("treats a loop as being in its own scope", () => {
    expect(loopsForNode(STRUCTURE, "refine")).toEqual(["refine"]);
  });

  it("finds a loop nested inside a plain group", () => {
    expect(loopsForNode(STRUCTURE, "batch.member")).toEqual(["batch"]);
  });

  it("gives nothing for a node no loop encloses", () => {
    expect(loopsForNode(STRUCTURE, "unrelated")).toEqual([]);
    expect(loopsForNode(STRUCTURE, "outer.plain")).toEqual([]);
  });
});

const pass = (iteration: number | undefined): LoopOption => ({ iteration, loopId: "batch" }) as LoopOption;

// [whole loop, then the passes newest first]
const OPTIONS = [pass(undefined), pass(3), pass(2), pass(1)];

describe("selectActivePass", () => {
  it("takes the pass the server resolved for the row", () => {
    // A mapped expansion's region index is its map slot, so the url cannot be read as a pass.
    // Slot 7 names no pass, and before this the level simply disappeared.
    const params = new URLSearchParams("region_index=7");

    expect(selectActivePass(OPTIONS, params, 1)?.iteration).toBe(1);
    // Reading the slot off the url is what used to hide the level.
    expect(OPTIONS.find((option) => option.iteration === 7)).toBeUndefined();
  });

  it("falls back to the newest pass when nothing pins one", () => {
    expect(selectActivePass(OPTIONS, new URLSearchParams())?.iteration).toBe(3);
  });

  it("reads a pass pinned as a filter", () => {
    expect(selectActivePass(OPTIONS, new URLSearchParams("iteration=1"))?.iteration).toBe(1);
  });

  it("ignores a pass belonging to a different loop", () => {
    const params = new URLSearchParams("loop_id=other&iteration=1");

    expect(selectActivePass(OPTIONS, params)?.iteration).toBe(3);
  });
});

describe("membersOfGroup", () => {
  it("collects a group's tasks even when they are not named after it", () => {
    // prefix_group_id=False: "improve" is inside "refine" without carrying its name.
    expect(membersOfGroup(STRUCTURE, "refine")).toEqual(new Set(["refine", "improve", "evaluate"]));
  });

  it("reaches a group nested inside another", () => {
    expect(membersOfGroup(STRUCTURE, "batch")).toEqual(new Set(["batch", "batch.member"]));
  });

  it("gives nothing for a group that is not there", () => {
    expect(membersOfGroup(STRUCTURE, "absent")).toEqual(new Set());
  });
});
