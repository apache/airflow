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

import type { EdgeResponse, NodeResponse } from "openapi/requests/types.gen";

import { generateElkGraph, hasUniformExternalConnectivity } from "./elkGraphUtils";
import type { FormattedNode } from "./elkGraphUtils";

// Minimal NodeResponse builder — fills the fields the layout pipeline actually reads.
const buildNode = (overrides: Partial<NodeResponse> & Pick<NodeResponse, "id" | "label">): NodeResponse => ({
  asset_condition_type: null,
  children: null,
  is_mapped: null,
  operator: null,
  setup_teardown_type: null,
  tooltip: null,
  type: "task",
  ...overrides,
});

const buildEdge = (sourceId: string, targetId: string): EdgeResponse => ({
  is_setup_teardown: null,
  label: null,
  source_id: sourceId,
  target_id: targetId,
});

describe("hasUniformExternalConnectivity", () => {
  it("returns false for the vanilla TaskGroup shape (only entry/exit cross the boundary)", () => {
    // ``a1`` has external source {start}; ``group_done`` has external target
    // {final_task}. Their profiles differ — not canonical fan-in/fan-out.
    const edges = [buildEdge("start", "a1"), buildEdge("group_done", "final_task")];
    const result = hasUniformExternalConnectivity(new Set(["a1", "branch", "a2", "a3", "group_done"]), edges);

    expect(result).toBe(false);
  });

  it("returns false when externally-connected children have mixed profiles (entry + exits)", () => {
    // The #67714 bug-trigger shape: a1 is the "entry" with external source
    // {start}, while a2/a3/group_done are "exits" with external target
    // {final_task}. Profiles differ → not canonical → return false so the
    // author's explicit escape edges remain visible.
    const edges = [
      buildEdge("start", "a1"),
      buildEdge("group_done", "final_task"),
      buildEdge("a2", "final_task"),
      buildEdge("a3", "final_task"),
    ];
    const result = hasUniformExternalConnectivity(new Set(["a1", "branch", "a2", "a3", "group_done"]), edges);

    expect(result).toBe(false);
  });

  it("returns true for the canonical fan-in/fan-out shape", () => {
    // Every child has the same external source AND the same external target —
    // the "cleanup group" pattern that the optimisation is designed for.
    const edges = [
      buildEdge("upstream", "T1"),
      buildEdge("upstream", "T2"),
      buildEdge("upstream", "T3"),
      buildEdge("T1", "downstream"),
      buildEdge("T2", "downstream"),
      buildEdge("T3", "downstream"),
    ];
    const result = hasUniformExternalConnectivity(new Set(["T1", "T2", "T3"]), edges);

    expect(result).toBe(true);
  });
});

describe("generateElkGraph — open TaskGroup with escape edges (#67714)", () => {
  // Mirrors the minimal reproducer from issue #67714:
  //
  //   start ─→ group_a { a1 ─→ branch ─→ [a2, a3] ─→ group_done } ─→ final_task
  //                                       ↘──── ↘────────┐
  //                                              ───────→ final_task
  //
  // ``a1`` is the entry (external source {start}), ``a2``/``a3``/``group_done``
  // are exits (external target {final_task}). Their profiles differ, so
  // ``hasUniformExternalConnectivity`` correctly returns false and the
  // open-group rewrite branch never runs — every internal and escape edge
  // is rendered individually.
  const internalChildren: Array<NodeResponse> = [
    buildNode({ id: "a1", label: "task_a1" }),
    buildNode({ id: "branch", label: "branch_a" }),
    buildNode({ id: "a2", label: "task_a2" }),
    buildNode({ id: "a3", label: "task_a3" }),
    buildNode({ id: "group_done", label: "group_done" }),
  ];

  const nodes: Array<NodeResponse> = [
    buildNode({ id: "start", label: "start" }),
    buildNode({
      children: internalChildren,
      id: "group_a",
      label: "group_a",
    }),
    buildNode({ id: "final_task", label: "final_task" }),
  ];

  const edges: Array<EdgeResponse> = [
    buildEdge("start", "a1"),
    buildEdge("a1", "branch"),
    buildEdge("branch", "a2"),
    buildEdge("branch", "a3"),
    buildEdge("a2", "group_done"),
    buildEdge("a3", "group_done"),
    buildEdge("group_done", "final_task"),
    // The two "escape" edges that trip the bug:
    buildEdge("a2", "final_task"),
    buildEdge("a3", "final_task"),
  ];

  it("keeps internal group edges so ELK can lay out the children", () => {
    const root = generateElkGraph({
      direction: "RIGHT",
      edges,
      font: "12px sans-serif",
      nodes,
      openGroupIds: ["group_a"],
    });

    const groupNode = (root.children as Array<FormattedNode>).find((child) => child.id === "group_a");

    expect(groupNode).toBeDefined();
    expect(groupNode?.isOpen).toBe(true);

    // All five internal edges must survive — that's what ELK needs to lay out
    // a1 → branch → [a2, a3] → group_done correctly inside the group.
    const internalEdgeIds = new Set(groupNode?.edges?.map((edge) => edge.id) ?? []);

    expect(internalEdgeIds).toEqual(
      new Set(["a1-branch", "branch-a2", "branch-a3", "a2-group_done", "a3-group_done"]),
    );
  });

  it("renders each crossing escape edge individually instead of collapsing them", () => {
    const root = generateElkGraph({
      direction: "RIGHT",
      edges,
      font: "12px sans-serif",
      nodes,
      openGroupIds: ["group_a"],
    });

    // The deliberately-wired escape edges from a2 and a3 must remain visible
    // so the author's explicit dependency intent is preserved in the graph.
    const rootEdgeIds = new Set(root.edges?.map((edge) => edge.id) ?? []);

    expect(rootEdgeIds).toEqual(
      new Set(["start-a1", "group_done-final_task", "a2-final_task", "a3-final_task"]),
    );
  });
});

describe("generateElkGraph — open TaskGroup matching the canonical fan-in/fan-out shape", () => {
  // The "cleanup group" pattern the optimisation is designed for: every child
  // has the SAME external source AND the SAME external target. The collapse
  // optimisation should still fire here, and ``preserveInternal: true`` must
  // keep any internal edges intact.
  it("collapses crossing edges to a single group-level edge while keeping internal edges", () => {
    const nodes: Array<NodeResponse> = [
      buildNode({ id: "upstream", label: "upstream" }),
      buildNode({
        children: [
          buildNode({ id: "T1", label: "T1" }),
          buildNode({ id: "T2", label: "T2" }),
          buildNode({ id: "T3", label: "T3" }),
          buildNode({ id: "T_internal", label: "T_internal" }),
        ],
        id: "cleanup_group",
        label: "cleanup_group",
      }),
      buildNode({ id: "downstream", label: "downstream" }),
    ];

    const edges: Array<EdgeResponse> = [
      buildEdge("upstream", "T1"),
      buildEdge("upstream", "T2"),
      buildEdge("upstream", "T3"),
      buildEdge("T1", "downstream"),
      buildEdge("T2", "downstream"),
      buildEdge("T3", "downstream"),
      // An internal edge within the group; must survive the optimisation so
      // ELK can lay it out inside the open group.
      buildEdge("T1", "T_internal"),
    ];

    const root = generateElkGraph({
      direction: "RIGHT",
      edges,
      font: "12px sans-serif",
      nodes,
      openGroupIds: ["cleanup_group"],
    });

    const groupNode = (root.children as Array<FormattedNode>).find((child) => child.id === "cleanup_group");

    // The internal T1 → T_internal edge must still be present in the group's
    // edges array (this is what the ``preserveInternal: true`` flag protects).
    const internalEdgeIds = new Set(groupNode?.edges?.map((edge) => edge.id) ?? []);

    expect(internalEdgeIds).toEqual(new Set(["T1-T_internal"]));

    // Six crossing edges (3 fan-in + 3 fan-out) collapse to one each.
    const rootEdgeIds = new Set(root.edges?.map((edge) => edge.id) ?? []);

    expect(rootEdgeIds).toEqual(new Set(["upstream-cleanup_group", "cleanup_group-downstream"]));
  });
});

describe("generateElkGraph — open TaskGroup without escape edges", () => {
  // Regression guard for the simple TaskGroup shape (only entry/exit cross the
  // boundary). ``hasUniformExternalConnectivity`` returns false here, so the
  // open-group rewrite branch never runs — internal edges should already survive.
  it("keeps internal group edges intact", () => {
    const nodes: Array<NodeResponse> = [
      buildNode({ id: "start", label: "start" }),
      buildNode({
        children: [
          buildNode({ id: "a1", label: "task_a1" }),
          buildNode({ id: "a2", label: "task_a2" }),
          buildNode({ id: "group_done", label: "group_done" }),
        ],
        id: "group_a",
        label: "group_a",
      }),
      buildNode({ id: "final_task", label: "final_task" }),
    ];

    const edges: Array<EdgeResponse> = [
      buildEdge("start", "a1"),
      buildEdge("a1", "a2"),
      buildEdge("a2", "group_done"),
      buildEdge("group_done", "final_task"),
    ];

    const root = generateElkGraph({
      direction: "RIGHT",
      edges,
      font: "12px sans-serif",
      nodes,
      openGroupIds: ["group_a"],
    });

    const groupNode = (root.children as Array<FormattedNode>).find((child) => child.id === "group_a");

    const internalEdgeIds = new Set(groupNode?.edges?.map((edge) => edge.id) ?? []);

    expect(internalEdgeIds).toEqual(new Set(["a1-a2", "a2-group_done"]));
  });
});

describe("generateElkGraph — closed TaskGroup", () => {
  // Closed-group behaviour must still drop internal edges (they're not laid out
  // when the group is collapsed) and rewrite crossings to point at the group.
  it("drops internal edges and rewrites crossings to the group", () => {
    const nodes: Array<NodeResponse> = [
      buildNode({ id: "start", label: "start" }),
      buildNode({
        children: [buildNode({ id: "a1", label: "task_a1" }), buildNode({ id: "a2", label: "task_a2" })],
        id: "group_a",
        label: "group_a",
      }),
      buildNode({ id: "final_task", label: "final_task" }),
    ];

    const edges: Array<EdgeResponse> = [
      buildEdge("start", "a1"),
      buildEdge("a1", "a2"),
      buildEdge("a2", "final_task"),
    ];

    const root = generateElkGraph({
      direction: "RIGHT",
      edges,
      font: "12px sans-serif",
      nodes,
      openGroupIds: [],
    });

    const rootEdgeIds = new Set(root.edges?.map((edge) => edge.id) ?? []);

    // Internal a1 → a2 must not appear at the root level; crossings collapse to
    // single group-level edges.
    expect(rootEdgeIds).toEqual(new Set(["start-group_a", "group_a-final_task"]));
  });
});
