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

import Color from "color";
import type { Node as ReactFlowNode } from "reactflow";
import type { ElkExtendedEdge } from "elkjs";

import type { SelectionProps } from "src/dag/useSelection";
import { getTask } from "src/utils";
import type { Task, TaskInstance, NodeType } from "src/types";

import type { CustomNodeProps } from "./Node";

interface FlattenNodesProps {
  children?: NodeType[];
  selected: SelectionProps;
  groups: Task;
  latestDagRunId: string;
  parent?: ReactFlowNode<CustomNodeProps>;
  openGroupIds: string[];
  onToggleGroups: (groupIds: string[]) => void;
  hoveredTaskState?: string | null;
  isZoomedOut: boolean;
}

// Generate a flattened list of nodes for react-flow to render
export const flattenNodes = ({
  children,
  selected,
  groups,
  latestDagRunId,
  onToggleGroups,
  openGroupIds,
  parent,
  hoveredTaskState,
  isZoomedOut,
}: FlattenNodesProps) => {
  let nodes: ReactFlowNode<CustomNodeProps>[] = [];
  let edges: ElkExtendedEdge[] = [];
  if (!children) return { nodes, edges };
  const parentNode = parent ? { parentNode: parent.id } : undefined;

  children.forEach((node) => {
    let instance: TaskInstance | undefined;
    const group = getTask({ taskId: node.id, task: groups });
    if (!node.id.endsWith("join_id") && selected.runId) {
      instance = group?.instances.find((ti) => ti.runId === selected.runId);
    }
    const isSelected = node.id === selected.taskId;
    const isActive =
      instance && hoveredTaskState !== undefined
        ? hoveredTaskState === instance.state
        : true;

    const newNode = {
      id: node.id,
      data: {
        width: node.width,
        height: node.height,
        task: group,
        instance,
        isSelected,
        latestDagRunId,
        isActive,
        isZoomedOut,
        onToggleCollapse: () => {
          let newGroupIds = [];
          if (!node.value.isOpen) {
            newGroupIds = [...openGroupIds, node.id];
          } else {
            newGroupIds = openGroupIds.filter((g) => g !== node.id);
          }
          onToggleGroups(newGroupIds);
        },
        ...node.value,
      },
      type: "custom",
      position: {
        x: node.x || 0,
        y: node.y || 0,
      },
      positionAbsolute: {
        x: (parent?.positionAbsolute?.x || 0) + (node.x || 0),
        y: (parent?.positionAbsolute?.y || 0) + (node.y || 0),
      },
      ...parentNode,
    };

    if (node.edges) {
      edges = [...edges, ...node.edges];
    }

    nodes.push(newNode);

    if (node.children) {
      const { nodes: childNodes, edges: childEdges } = flattenNodes({
        children: node.children,
        selected,
        groups,
        latestDagRunId,
        onToggleGroups,
        openGroupIds,
        parent: newNode,
        hoveredTaskState,
        isZoomedOut,
      });
      nodes = [...nodes, ...childNodes];
      edges = [...edges, ...childEdges];
    }
  });
  return {
    nodes,
    edges,
  };
};

export const nodeColor = ({
  data: { height, width, instance, isActive, isOpen },
}: ReactFlowNode<CustomNodeProps>) => {
  let opacity = "90";
  let color = "#cccccc";
  if (!height || !width) return "";
  if (instance?.state && !isOpen)
    color = Color(stateColors[instance.state]).hex();
  if (isOpen) opacity = "50";
  if (!isActive) opacity = "21";

  return `${color}${opacity}`;
};

export const nodeStrokeColor = (
  { data: { isSelected } }: ReactFlowNode<CustomNodeProps>,
  colors: Record<string, string>
) => (isSelected ? colors.blue[500] : "");

interface Edge extends ElkExtendedEdge {
  parentNode?: string;
}

interface BuildEdgesProps {
  edges?: Edge[];
  nodes: ReactFlowNode<CustomNodeProps>[];
  selectedTaskId?: string | null;
  isZoomedOut?: boolean;
}

// Format edge data to what react-flow needs to render successfully
export const buildEdges = ({
  edges = [],
  nodes,
  selectedTaskId,
  isZoomedOut,
}: BuildEdgesProps) =>
  edges
    .map((edge) => ({
      id: edge.id,
      source: edge.sources[0],
      target: edge.targets[0],
      data: { rest: edge },
      type: "custom",
    }))
    .map((e) => {
      const isSelected =
        selectedTaskId &&
        (e.source === selectedTaskId || e.target === selectedTaskId);

      if (e.data.rest?.parentNode) {
        const parentNode = nodes.find((n) => n.id === e.data.rest.parentNode);
        const parentX =
          parentNode?.positionAbsolute?.x || parentNode?.position.x || 0;
        const parentY =
          parentNode?.positionAbsolute?.y || parentNode?.position.y || 0;
        return {
          ...e,
          data: {
            rest: {
              isZoomedOut,
              ...e.data.rest,
              labels: e.data.rest.labels?.map((l) =>
                l.x && l.y ? { ...l, x: l.x + parentX, y: l.y + parentY } : l
              ),
              isSelected,
              sections: e.data.rest.sections.map((s) => ({
                ...s,
                startPoint: {
                  x: s.startPoint.x + parentX,
                  y: s.startPoint.y + parentY,
                },
                endPoint: {
                  x: s.endPoint.x + parentX,
                  y: s.endPoint.y + parentY,
                },
                bendPoints: (s.bendPoints || []).map((bp) => ({
                  x: bp.x + parentX,
                  y: bp.y + parentY,
                })),
              })),
            },
          },
        };
      }

      return {
        ...e,
        data: {
          rest: {
            ...e.data.rest,
            isSelected,
            isZoomedOut,
          },
        },
      };
    });
