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

import { getHighlightedLineage } from "./lineageHighlightUtils";

const edges = [
  { source_id: "upstream_dag", target_id: "upstream_dag.producer_task" },
  { source_id: "upstream_dag.producer_task", target_id: "asset:1" },
  { source_id: "asset:1", target_id: "downstream_dag.consumer_task" },
  { source_id: "downstream_dag.consumer_task", target_id: "asset:2" },
];

describe("getHighlightedLineage", () => {
  it("returns empty highlights when nodeId is undefined", () => {
    const result = getHighlightedLineage({
      direction: "downstream",
      edges,
    });

    expect(result.highlightedEdgeIds.size).toBe(0);
    expect(result.highlightedNodeIds.size).toBe(0);
  });

  it("highlights the full upstream lineage for a node", () => {
    const result = getHighlightedLineage({
      direction: "upstream",
      edges,
      nodeId: "asset:1",
    });

    expect(result.highlightedEdgeIds).toEqual(
      new Set(["upstream_dag-upstream_dag.producer_task", "upstream_dag.producer_task-asset:1"]),
    );
    expect(result.highlightedNodeIds).toEqual(
      new Set(["asset:1", "upstream_dag.producer_task", "upstream_dag"]),
    );
  });

  it("highlights the full downstream lineage for a node", () => {
    const result = getHighlightedLineage({
      direction: "downstream",
      edges,
      nodeId: "asset:1",
    });

    expect(result.highlightedEdgeIds).toEqual(
      new Set(["asset:1-downstream_dag.consumer_task", "downstream_dag.consumer_task-asset:2"]),
    );
    expect(result.highlightedNodeIds).toEqual(
      new Set(["asset:1", "downstream_dag.consumer_task", "asset:2"]),
    );
  });

  it("handles cyclic lineage without infinite traversal", () => {
    const cycleResult = getHighlightedLineage({
      direction: "downstream",
      edges: [
        { source_id: "asset:1", target_id: "asset:2" },
        { source_id: "asset:2", target_id: "asset:1" },
      ],
      nodeId: "asset:1",
    });

    expect(cycleResult.highlightedEdgeIds).toEqual(new Set(["asset:1-asset:2", "asset:2-asset:1"]));
    expect(cycleResult.highlightedNodeIds).toEqual(new Set(["asset:1", "asset:2"]));
  });
});
