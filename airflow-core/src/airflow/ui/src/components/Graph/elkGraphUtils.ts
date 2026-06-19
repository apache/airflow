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
  team?: string | null;
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
 * Returns true when every child task with at least one external connection
 * shares the **exact same full external profile** — the same set of external
 * sources AND the same set of external targets — as every other externally-
 * connected child.
 *
 * Canonical pattern — a "cleanup" group where every task fans out from one
 * upstream node and funnels into the same downstream node:
 *
 *   upstream → T1 ─┐
 *   upstream → T2 ─┼→ downstream
 *   upstream → T3 ─┘
 *
 * Here all three children have profile ``({upstream}, {downstream})`` — same
 * sources, same targets. Rendering N individual crossing edges adds visual
 * noise without conveying any extra information beyond "the group connects
 * upstream → downstream". When this returns true, the caller collapses those
 * N edges to a single group-level edge while still rendering the children.
 *
 * Mixed profiles — e.g. one child is the group's "entry" (external sources
 * only, no external targets) while others are "exits" (external targets only,
 * no external sources) — are NOT canonical. The author has expressed
 * deliberately different external connectivity per child, and collapsing
 * those edges would hide that intent. The check returns false in that case,
 * and the caller renders each crossing edge individually. See #67714.
 *
 * Uses the original, unmodified edge list so that prior sibling group
 * transformations do not affect the connectivity check.
 */
export const hasUniformExternalConnectivity = (
  childIdSet: Set<string>,
  edges: Array<EdgeResponse>,
): boolean => {
  // For each externally-connected child, build the full ``(sources, targets)``
  // profile in a single map (rather than tracking sources and targets in
  // independent maps — which loses the per-child correlation).
  const profileByChild = new Map<string, { sources: Set<string>; targets: Set<string> }>();
  const getOrInitProfile = (childId: string) => {
    let profile = profileByChild.get(childId);

    if (profile === undefined) {
      profile = { sources: new Set<string>(), targets: new Set<string>() };
      profileByChild.set(childId, profile);
    }

    return profile;
  };

  for (const edge of edges) {
    const sourceIsChild = childIdSet.has(edge.source_id);
    const targetIsChild = childIdSet.has(edge.target_id);

    if (!sourceIsChild && targetIsChild) {
      getOrInitProfile(edge.target_id).sources.add(edge.source_id);
    }

    if (sourceIsChild && !targetIsChild) {
      getOrInitProfile(edge.source_id).targets.add(edge.target_id);
    }
  }

  // Need at least 2 externally-connected children for the optimisation to be
  // worthwhile — one child has nothing to collapse against.
  if (profileByChild.size < 2) {
    return false;
  }

  // All externally-connected children must share the exact same profile.
  const [reference, ...rest] = [...profileByChild.values()];

  // The early-return above on ``profileByChild.size < 2`` guarantees that the
  // destructure produced a defined ``reference``, but TypeScript can't see
  // through the map-size guard. This explicit check both narrows the type and
  // documents the invariant.
  if (reference === undefined) {
    return false;
  }

  const setsEqual = (left: Set<string>, right: Set<string>) => {
    if (left.size !== right.size) {
      return false;
    }
    for (const value of left) {
      if (!right.has(value)) {
        return false;
      }
    }

    return true;
  };

  return rest.every(
    (profile) =>
      setsEqual(profile.sources, reference.sources) && setsEqual(profile.targets, reference.targets),
  );
};

// ---------------------------------------------------------------------------
// Edge rewriting helper
// ---------------------------------------------------------------------------

type RewriteGroupEdgesProps = {
  childIdSet: Set<string>;
  edges: Array<EdgeResponse>;
  groupId: string;
  /**
   * When false (the default, used for *closed* groups), purely-internal edges
   * are dropped — the collapsed group does not need its internal layout.
   *
   * When true (used when applying the uniform-external optimisation to an
   * *open* group), internal edges pass through unchanged so the caller can
   * still extract them as the group's internal edges; only crossing edges get
   * rewritten and deduplicated.
   */
  preserveInternal?: boolean;
};

/**
 * Rewrites crossing edges so both endpoints reference `groupId` instead of a
 * child node, then deduplicates the result so N rewritten edges collapse to
 * one per (source, target) pair.
 */
const rewriteGroupEdges = ({
  childIdSet,
  edges,
  groupId,
  preserveInternal = false,
}: RewriteGroupEdgesProps): Array<EdgeResponse> => {
  const seen = new Set<string>();

  return edges
    .filter((fe) => preserveInternal || !(childIdSet.has(fe.source_id) && childIdSet.has(fe.target_id)))
    .map((fe) => {
      const sourceIsChild = childIdSet.has(fe.source_id);
      const targetIsChild = childIdSet.has(fe.target_id);

      // Internal edges of an open group must pass through unchanged so the
      // caller can recognise and extract them. Rewriting both endpoints to
      // ``groupId`` would (a) collapse them to a self-loop and (b) hide them
      // from the subsequent internal-edge extraction loop.
      if (preserveInternal && sourceIsChild && targetIsChild) {
        return fe;
      }

      return {
        ...fe,
        source_id: sourceIsChild ? groupId : fe.source_id,
        target_id: targetIsChild ? groupId : fe.target_id,
      };
    })
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
      //
      // ``preserveInternal: true`` is required because the group is *open* — its
      // internal edges must survive past the rewrite so the extraction loop
      // below can pull them into the group's ``edges`` array. Without it, ELK
      // would receive an open group with no internal edges and fail to lay out
      // the children in a sensible left-to-right order whenever an internal
      // task has a direct dependency on a node outside the group (see #67714).
      if (hasUniformExternalConnectivity(childIdSet, unformattedEdges)) {
        filteredEdges = rewriteGroupEdges({
          childIdSet,
          edges: filteredEdges,
          groupId: node.id,
          preserveInternal: true,
        });
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
      filteredEdges = rewriteGroupEdges({
        childIdSet: new Set(childIds),
        edges: filteredEdges,
        groupId: node.id,
      });
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
      team: node.team,
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
