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

import { getGatePathEdgeIdsForSelection } from "./reactflowUtils";

const node = (id: string, type: string) => ({ id, type });
const edge = (id: string, source: string, target: string) => ({ id, source, target });

describe("getGatePathEdgeIdsForSelection", () => {
  it("returns the edges on the path from a selected asset through a gate to the dag", () => {
    const nodes = [node("asset:1", "asset"), node("gate-0", "asset-condition"), node("dag:my_dag", "dag")];
    const edges = [edge("e1", "asset:1", "gate-0"), edge("e2", "gate-0", "dag:my_dag")];

    const result = getGatePathEdgeIdsForSelection(nodes, edges, (id) => id === "asset:1");

    expect(result).toEqual(new Set(["e1", "e2"]));
  });

  it("does not highlight a sibling asset's edge in the same AND/OR condition", () => {
    // asset:1 and asset:2 both feed and-gate-0, which schedules dag:my_dag. Selecting asset:1
    // must only highlight its own edge into the gate and the gate's single output -- not
    // asset:2's edge, even though asset:2 shares the same gate.
    const nodes = [
      node("asset:1", "asset"),
      node("asset:2", "asset"),
      node("and-gate-0", "asset-condition"),
      node("dag:my_dag", "dag"),
    ];
    const edges = [
      edge("e1", "asset:1", "and-gate-0"),
      edge("e2", "asset:2", "and-gate-0"),
      edge("e3", "and-gate-0", "dag:my_dag"),
    ];

    const result = getGatePathEdgeIdsForSelection(nodes, edges, (id) => id === "asset:1");

    expect(result).toEqual(new Set(["e1", "e3"]));
    expect(result.has("e2")).toBe(false);
  });

  it("fans out to every input when the Dag (output side) is selected", () => {
    // From the Dag's perspective, all of the gate's inputs are genuinely relevant.
    const nodes = [
      node("asset:1", "asset"),
      node("asset:2", "asset"),
      node("and-gate-0", "asset-condition"),
      node("dag:my_dag", "dag"),
    ];
    const edges = [
      edge("e1", "asset:1", "and-gate-0"),
      edge("e2", "asset:2", "and-gate-0"),
      edge("e3", "and-gate-0", "dag:my_dag"),
    ];

    const result = getGatePathEdgeIdsForSelection(nodes, edges, (id) => id === "dag:my_dag");

    expect(result).toEqual(new Set(["e1", "e2", "e3"]));
  });

  it("walks forward through nested gates without fanning into a sibling gate's inputs", () => {
    // asset:1 feeds or-gate-1, which is one input (among others) to and-gate-0, which schedules
    // the Dag. Selecting asset:1 should light up its single path all the way to the Dag, but not
    // and-gate-0's other inputs.
    const nodes = [
      node("asset:1", "asset"),
      node("asset:2", "asset"),
      node("or-gate-1", "asset-condition"),
      node("and-gate-0", "asset-condition"),
      node("dag:my_dag", "dag"),
    ];
    const edges = [
      edge("e1", "asset:1", "or-gate-1"),
      edge("e2", "or-gate-1", "and-gate-0"),
      edge("e3", "asset:2", "and-gate-0"),
      edge("e4", "and-gate-0", "dag:my_dag"),
    ];

    const result = getGatePathEdgeIdsForSelection(nodes, edges, (id) => id === "asset:1");

    expect(result).toEqual(new Set(["e1", "e2", "e4"]));
    expect(result.has("e3")).toBe(false);
  });

  it("returns an empty set when the graph has no gate nodes", () => {
    const nodes = [node("asset:1", "asset"), node("dag:my_dag", "dag")];
    const edges = [edge("e1", "asset:1", "dag:my_dag")];

    const result = getGatePathEdgeIdsForSelection(nodes, edges, (id) => id === "asset:1");

    expect(result).toEqual(new Set());
  });

  it("returns an empty set when nothing is selected", () => {
    const nodes = [node("asset:1", "asset"), node("gate-0", "asset-condition"), node("dag:my_dag", "dag")];
    const edges = [edge("e1", "asset:1", "gate-0"), edge("e2", "gate-0", "dag:my_dag")];

    const result = getGatePathEdgeIdsForSelection(nodes, edges, () => false);

    expect(result).toEqual(new Set());
  });
});
