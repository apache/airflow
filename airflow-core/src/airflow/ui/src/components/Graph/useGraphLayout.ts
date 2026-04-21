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
import { createListCollection } from "@chakra-ui/react";
import { useQuery } from "@tanstack/react-query";
import ELK, { type ElkNode } from "elkjs";
// ?raw imports the file content as a plain string without any transformation.
// We create a blob: URL from it so the Worker is always same-origin to the
// page — avoiding the cross-origin SecurityError that occurs in Airflow's dev
// setup where Vite (5173) and Flask (28080) run on different ports, and all
// URL-based worker approaches (?worker, ?worker&inline, new URL()) resolve to
// the Vite origin which browsers reject for Workers.
import ElkWorkerSource from "elkjs/lib/elk-worker.min.js?raw";
import type { TFunction } from "i18next";

import type { NodeResponse, StructureDataResponse } from "openapi/requests/types.gen";

import { generateElkGraph } from "./elkGraphUtils";
import { flattenGraph, formatFlowEdges } from "./reactflowUtils";

// Blob URL created once at module load. `type: "classic"` preserves the
// original CJS environment detection in elk-worker: as a classic script
// `typeof module === "undefined"`, so the worker sets self.onmessage rather
// than exporting a FakeWorker.
const elkWorkerBlobUrl = URL.createObjectURL(new Blob([ElkWorkerSource], { type: "application/javascript" }));

const elk = new ELK({
  workerFactory: () => new Worker(elkWorkerBlobUrl, { type: "classic" }),
});

export type Direction = "DOWN" | "LEFT" | "RIGHT" | "UP";
export const directionOptions = (translate: TFunction) =>
  createListCollection({
    items: [
      { label: translate("graph.directionRight"), value: "RIGHT" as Direction },
      { label: translate("graph.directionLeft"), value: "LEFT" as Direction },
      { label: translate("graph.directionUp"), value: "UP" as Direction },
      { label: translate("graph.directionDown"), value: "DOWN" as Direction },
    ],
  });

export type LayoutNode = ElkNode & NodeResponse;

type LayoutProps = {
  direction: Direction;
  openGroupIds: Array<string>;
  versionNumber?: number;
} & StructureDataResponse;

export const useGraphLayout = ({
  direction = "RIGHT",
  edges,
  nodes,
  openGroupIds = [],
  versionNumber,
}: LayoutProps) =>
  useQuery({
    queryFn: async () => {
      const font = `bold 18px ${globalThis.getComputedStyle(document.body).fontFamily}`;

      // 1. Format graph data to pass for elk to process
      const graph = generateElkGraph({
        direction,
        edges,
        font,
        nodes,
        openGroupIds,
      });

      // 2. use elk to generate the size and position of nodes and edges
      const data = (await elk.layout(graph)) as LayoutNode;

      // 3. Flatten the nodes and edges for xyflow to actually render the graph
      const flattenedData = flattenGraph({
        children: (data.children ?? []) as Array<LayoutNode>,
      });

      // merge & dedupe edges — O(n) via Map (first occurrence wins) rather than
      // O(n²) findIndex. Root-level edges from ELK come first; child edges from
      // flattenedData are skipped when the same id is already present.
      const seenEdgeIds = new Set<string>();
      const flatEdges = [...(data.edges ?? []), ...flattenedData.edges].filter((edge) => {
        if (seenEdgeIds.has(edge.id)) {
          return false;
        }
        seenEdgeIds.add(edge.id);

        return true;
      });

      const formattedEdges = formatFlowEdges({ edges: flatEdges });

      return { edges: formattedEdges, nodes: flattenedData.nodes };
    },
    queryKey: ["graphLayout", nodes, openGroupIds, versionNumber, edges, direction],
  });
