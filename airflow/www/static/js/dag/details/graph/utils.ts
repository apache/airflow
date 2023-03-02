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
import type { Task, TaskInstance } from "src/types";
import type { NodeType } from "src/datasets/Graph/Node";

import type { CustomNodeProps } from "./Node";

interface FlattenNodesProps {
  children: NodeType[];
  selected: SelectionProps;
  groups: Task;
  latestDagRunId: string;
  parent?: ReactFlowNode<CustomNodeProps>;
  openGroupIds: string[];
  onToggleGroups: (groupIds: string[]) => void;
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
}: FlattenNodesProps) => {
  let nodes: ReactFlowNode<CustomNodeProps>[] = [];
  const parentNode = parent ? { parentNode: parent.id } : undefined;
  children.forEach((node) => {
    let instance: TaskInstance | undefined;
    const group = getTask({ taskId: node.id, task: groups });
    if (!node.id.includes("join_id") && selected.runId) {
      instance = group?.instances.find((ti) => ti.runId === selected.runId);
    }
    const isSelected = node.id === selected.taskId && !!instance;

    const newNode = {
      id: node.id,
      data: {
        width: node.width,
        height: node.height,
        task: group,
        instance,
        isSelected,
        latestDagRunId,
        onToggleCollapse: () => {
          let newGroupIds = [];
          if (!node.value.isOpen) {
            newGroupIds = [...openGroupIds, node.value.label];
          } else {
            newGroupIds = openGroupIds.filter((g) => g !== node.value.label);
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

    nodes.push(newNode);

    if (node.children) {
      const childNodes = flattenNodes({
        children: node.children,
        selected,
        groups,
        latestDagRunId,
        onToggleGroups,
        openGroupIds,
        parent: newNode,
      });
      nodes = [...nodes, ...childNodes];
    }
  });
  return nodes;
};

export const nodeColor = ({
  data: { height, width, instance },
}: ReactFlowNode<CustomNodeProps>) => {
  if (!height || !width) return "";
  if (width > 200 || height > 60) {
    return "#cccccc50";
  }
  if (instance?.state) {
    return `${Color(stateColors[instance.state]).hex()}90`;
  }
  return "#cccccc90";
};

export const nodeStrokeColor = (
  { data: { isSelected } }: ReactFlowNode<CustomNodeProps>,
  colors: any
) => (isSelected ? colors.blue[500] : "");

interface BuildEdgesProps {
  edges?: ElkExtendedEdge[];
  nodes: ReactFlowNode<CustomNodeProps>[];
  selectedTaskId?: string | null;
}

// Format edge data to what react-flow needs to render successfully
export const buildEdges = ({
  edges = [],
  nodes,
  selectedTaskId,
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
      const sourceIds = e.source.split(".");
      const targetIds = e.target.split(".");
      const isSelected =
        selectedTaskId &&
        (e.source === selectedTaskId || e.target === selectedTaskId);

      if (
        sourceIds.length === targetIds.length &&
        sourceIds[0] === targetIds[0]
      ) {
        const parentIds =
          sourceIds.length > targetIds.length ? sourceIds : targetIds;
        parentIds.pop();
        let parentX = 0;
        let parentY = 0;

        nodes
          .filter((n) => parentIds.some((p) => p === n.data.label))
          .forEach((p) => {
            parentX += p.position.x;
            parentY += p.position.y;
          });

        return {
          ...e,
          data: {
            rest: {
              ...e.data.rest,
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
          },
        },
      };
    });
