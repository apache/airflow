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
import type { ElkNode, ElkExtendedEdge, ElkShape } from "elkjs";

import type { EdgeResponse, NodeResponse } from "openapi/requests/types.gen";

import type { Direction } from "./DirectionDropdown";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type EdgeLabel = {
  height: number;
  id: string;
  text: string;
  width: number;
};

export type FormattedEdge = {
  id: string;
  isSetupTeardown?: boolean;
  labels?: Array<EdgeLabel>;
  parentNode?: string;
} & ElkExtendedEdge;

export type FormattedNode = {
  assetCondition?: NodeResponse["asset_condition_type"];
  childCount?: number;
  edges?: Array<FormattedEdge>;
  isGroup: boolean;
  isMapped?: boolean;
  isOpen?: boolean;
  setupTeardownType?: NodeResponse["setup_teardown_type"];
} & ElkShape &
  NodeResponse;

export type GenerateElkProps = {
  direction: Direction;
  edges: Array<EdgeResponse>;
  font: string;
  nodes: Array<NodeResponse>;
  openGroupIds?: Array<string>;
};

// ---------------------------------------------------------------------------
// Canvas singleton for text measurement
// ---------------------------------------------------------------------------

// Single reusable canvas context — avoids allocating a new HTMLCanvasElement
// on every call (once per node per layout run).
const canvasContext = document.createElement("canvas").getContext("2d");

const getTextWidth = (text: string, font: string) => {
  if (canvasContext) {
    canvasContext.font = font;

    return Math.max(200, canvasContext.measureText(text).width);
  }

  return Math.max(200, text.length * 9);
};

// ---------------------------------------------------------------------------
// Edge helpers
// ---------------------------------------------------------------------------

const formatElkEdge = (edge: EdgeResponse, font: string, node?: NodeResponse): FormattedEdge => ({
  id: `${edge.source_id}-${edge.target_id}`,
  isSetupTeardown: edge.is_setup_teardown === null ? undefined : edge.is_setup_teardown,
  labels:
    edge.label === undefined || edge.label === null
      ? []
      : [
          {
            height: 16,
            id: edge.label,
            text: edge.label,
            width: getTextWidth(edge.label, font),
          },
        ],
  parentNode: node?.id,
  sources: [edge.source_id],
  targets: [edge.target_id],
});

/**
 * Returns true when every child task that has at least one external connection
 * shares exactly the same set of external sources AND the same set of external
 * targets as every other externally-connected child.
 *
 * Example — a "cleanup" group where every task fans out from one upstream node
 * and funnels into the same downstream node:
 *
 *   upstream → T1 ─┐
 *   upstream → T2 ─┼→ downstream
 *   upstream → T3 ─┘
 *
 * Rendering N individual crossing edges adds visual noise without conveying
 * any extra information beyond "the group connects upstream → downstream".
 * When this returns true, the caller collapses those N edges to a single
 * group-level edge while still rendering the children inside the group.
 *
 * Uses the original, unmodified edge list so that prior sibling group
 * transformations do not affect the connectivity check.
 */
export const hasUniformExternalConnectivity = (
  childIdSet: Set<string>,
  edges: Array<EdgeResponse>,
): boolean => {
  const sourcesPerChild = new Map<string, Set<string>>();
  const targetsPerChild = new Map<string, Set<string>>();

  for (const edge of edges) {
    const sourceIsChild = childIdSet.has(edge.source_id);
    const targetIsChild = childIdSet.has(edge.target_id);

    if (!sourceIsChild && targetIsChild) {
      const existing = sourcesPerChild.get(edge.target_id) ?? new Set<string>();

      existing.add(edge.source_id);
      sourcesPerChild.set(edge.target_id, existing);
    }

    if (sourceIsChild && !targetIsChild) {
      const existing = targetsPerChild.get(edge.source_id) ?? new Set<string>();

      existing.add(edge.target_id);
      targetsPerChild.set(edge.source_id, existing);
    }
  }

  // Need at least 2 children with external connections on at least one side
  // for the optimisation to be worthwhile.
  if (sourcesPerChild.size < 2 && targetsPerChild.size < 2) {
    return false;
  }

  // Build the union of all external sources / targets across all children.
  const allSources = new Set<string>();
  const allTargets = new Set<string>();

  for (const sources of sourcesPerChild.values()) {
    for (const source of sources) {
      allSources.add(source);
    }
  }
  for (const targets of targetsPerChild.values()) {
    for (const target of targets) {
      allTargets.add(target);
    }
  }

  // Every child's external sources must equal allSources (same size sufficient
  // given allSources is already the union — a child with fewer differs in size).
  for (const sources of sourcesPerChild.values()) {
    if (sources.size !== allSources.size) {
      return false;
    }
  }
  for (const targets of targetsPerChild.values()) {
    if (targets.size !== allTargets.size) {
      return false;
    }
  }

  return true;
};

// ---------------------------------------------------------------------------
// Edge rewriting helper
// ---------------------------------------------------------------------------

/**
 * Given the current working edge list, drops purely-internal edges, rewrites
 * crossing edges so both endpoints reference `groupId` instead of a child node,
 * then deduplicates the result so N rewritten edges collapse to one per
 * (source, target) pair.
 */
const rewriteGroupEdges = (
  edges: Array<EdgeResponse>,
  childIdSet: Set<string>,
  groupId: string,
): Array<EdgeResponse> => {
  const seen = new Set<string>();

  return edges
    .filter((fe) => !(childIdSet.has(fe.source_id) && childIdSet.has(fe.target_id)))
    .map((fe) => ({
      ...fe,
      source_id: childIdSet.has(fe.source_id) ? groupId : fe.source_id,
      target_id: childIdSet.has(fe.target_id) ? groupId : fe.target_id,
    }))
    .filter((fe) => {
      const key = `${fe.source_id}-${fe.target_id}`;

      if (seen.has(key)) {
        return false;
      }
      seen.add(key);

      return true;
    });
};

// ---------------------------------------------------------------------------
// Node helpers
// ---------------------------------------------------------------------------

const getNestedChildIds = (children: Array<NodeResponse>): Array<string> => {
  const childIds: Array<string> = [];

  for (const child of children) {
    childIds.push(child.id);
    if (child.children) {
      childIds.push(...getNestedChildIds(child.children));
    }
  }

  return childIds;
};

// ---------------------------------------------------------------------------
// Main graph builder
// ---------------------------------------------------------------------------

export const generateElkGraph = ({
  direction,
  edges: unformattedEdges,
  font,
  nodes,
  openGroupIds,
}: GenerateElkProps): ElkNode => {
  let filteredEdges = unformattedEdges;

  const formatChildNode = (node: NodeResponse): FormattedNode => {
    const isOpen = openGroupIds?.includes(node.id);
    const childCount = node.children?.filter((child) => child.type !== "join").length ?? 0;
    const childIds =
      node.children === null || node.children === undefined ? [] : getNestedChildIds(node.children);

    if (isOpen && node.children !== null && node.children !== undefined) {
      const childIdSet = new Set(childIds);

      // Process children first — their formatChildNode calls may modify filteredEdges
      // (removing edges that belong to nested open groups).
      const formattedChildren = node.children.map(formatChildNode);

      // If every externally-connected task shares the same upstream source(s)
      // and downstream target(s), collapse N crossing edges to one group-level
      // edge (same as a closed group) while keeping the children visible.
      // Checked against unformattedEdges so prior sibling transforms don't interfere.
      if (hasUniformExternalConnectivity(childIdSet, unformattedEdges)) {
        filteredEdges = rewriteGroupEdges(filteredEdges, childIdSet, node.id);
      }

      // Extract any remaining internal edges (both endpoints inside this group).
      const internalEdges: Array<FormattedEdge> = [];

      filteredEdges = filteredEdges.filter((edge) => {
        if (childIdSet.has(edge.source_id) && childIdSet.has(edge.target_id)) {
          internalEdges.push(formatElkEdge(edge, font, node));

          return false;
        }

        return true;
      });

      return {
        ...node,
        childCount,
        children: formattedChildren,
        edges: internalEdges,
        id: node.id,
        isGroup: true,
        isOpen,
        label: node.label,
        layoutOptions: {
          "elk.padding": "[top=80,left=15,bottom=15,right=15]",
          ...(direction === "RIGHT" ? { "elk.portConstraints": "FIXED_SIDE" } : {}),
        },
      };
    }

    if (!isOpen && node.children !== undefined) {
      // Use a Set for O(1) membership checks — childIds.includes() would be
      // O(n) per edge, turning the filter/map into O(n × E) for large groups.
      filteredEdges = rewriteGroupEdges(filteredEdges, new Set(childIds), node.id);
    }

    const label = `${node.label}${node.is_mapped ? "[1000]" : ""}${node.children ? ` + ${node.children.length} tasks` : ""}`;
    let width = getTextWidth(label, font);
    const hasStateBar = Boolean(node.is_mapped) || Boolean(node.children);
    let height = hasStateBar ? 90 : 80;

    if (node.type === "join") {
      width = 10;
      height = 10;
    } else if (node.type === "asset-condition") {
      width = 30;
      height = 30;
    }

    return {
      assetCondition: node.asset_condition_type,
      childCount,
      height,
      id: node.id,
      isGroup: Boolean(node.children),
      isMapped: node.is_mapped === null ? undefined : node.is_mapped,
      label: node.label,
      layoutOptions: direction === "RIGHT" ? { "elk.portConstraints": "FIXED_SIDE" } : undefined,
      operator: node.operator,
      setupTeardownType: node.setup_teardown_type,
      tooltip: node.tooltip,
      type: node.type,
      width,
    };
  };

  const children = nodes.map(formatChildNode);
  const edges = filteredEdges.map((fe) => formatElkEdge(fe, font));

  return {
    children: children as Array<ElkNode>,
    edges,
    id: "root",
    layoutOptions: {
      "elk.core.options.EdgeLabelPlacement": "CENTER",
      "elk.direction": direction,
      // SIMPLE placement is a single-pass algorithm — much faster than the
      // default BRANDES_KOEPF four-pass approach with acceptable quality for DAGs.
      "elk.layered.nodePlacement.strategy": "SIMPLE",
      // Crossing minimisation thoroughness (default 7) controls how many random
      // sweeps are attempted. Drop to 3 for large graphs where the extra passes
      // rarely pay off and add noticeable layout latency.
      ...(filteredEdges.length > 100 ? { "elk.layered.thoroughness": "3" } : {}),
      // hierarchyHandling is only needed when open groups create cross-hierarchy
      // edges. For flat DAGs (no open groups) omitting it simplifies ELK's task.
      ...(Boolean(openGroupIds?.length) ? { hierarchyHandling: "INCLUDE_CHILDREN" } : {}),
      "spacing.edgeLabel": "10.0",
    },
  };
};
