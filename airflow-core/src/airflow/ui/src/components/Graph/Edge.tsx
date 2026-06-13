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
import { Text, useToken } from "@chakra-ui/react";
import { Group } from "@visx/group";
import { LinePath } from "@visx/shape";
import { BaseEdge, getSmoothStepPath, useNodesData, useStore } from "@xyflow/react";
import type { Edge as EdgeType, EdgeProps } from "@xyflow/react";
import type { ElkPoint } from "elkjs";

import { opacityStyle } from "./graphTypes";
import type { EdgeData } from "./reactflowUtils";

type Props = EdgeProps<EdgeType<EdgeData>>;

const CustomEdge = ({
  data,
  id,
  markerEnd,
  markerStart,
  source,
  sourcePosition,
  sourceX,
  sourceY,
  target,
  targetPosition,
  targetX,
  targetY,
}: Props) => {
  const [
    strokeColor,
    blueColor,
    dataEdgeColor,
    draggingStrokeColor,
    draggingBlueColor,
    draggingDataEdgeColor,
  ] = useToken("colors", ["border.inverted", "blue.500", "purple.500", "border", "blue.400", "purple.400"]);

  // Read isSelected directly from the node store so that selection changes
  // don't require the parent to rebuild and pass down a new edges array.
  // useNodesData subscribes to data changes for these specific node IDs only.
  const nodesData = useNodesData([source, target]);
  const isSelected = nodesData.some((node) => Boolean(node.data.isSelected));
  const isConnectedNodeDragging = useStore((state) => {
    const sourceDragging = state.nodeLookup.get(source)?.internals.userNode.dragging ?? false;
    const targetDragging = state.nodeLookup.get(target)?.internals.userNode.dragging ?? false;

    return sourceDragging || targetDragging;
  });

  if (data === undefined) {
    return undefined;
  }
  const { isManualLayout = false, rest } = data;
  const isDragPreview = isManualLayout && isConnectedNodeDragging;
  const selectedEdgeStrokeColor = rest.edgeType === "data" ? dataEdgeColor : blueColor;
  const selectedDraggingEdgeStrokeColor =
    rest.edgeType === "data" ? draggingDataEdgeColor : draggingBlueColor;
  let edgeStrokeColor = (isSelected ? selectedEdgeStrokeColor : strokeColor) ?? "currentColor";

  if (isDragPreview) {
    edgeStrokeColor = (isSelected ? selectedDraggingEdgeStrokeColor : draggingStrokeColor) ?? "currentColor";
  }

  if (isManualLayout) {
    const [path] = getSmoothStepPath({
      sourcePosition,
      sourceX,
      sourceY,
      targetPosition,
      targetX,
      targetY,
    });

    return (
      <g {...opacityStyle(rest.isFiltered)} pointerEvents="none">
        <BaseEdge
          id={id}
          interactionWidth={0}
          markerEnd={markerEnd}
          markerStart={markerStart}
          path={path}
          style={{
            stroke: edgeStrokeColor,
            strokeDasharray: rest.isSetupTeardown ? "10,5" : undefined,
            strokeWidth: isSelected ? 3 : 1,
          }}
        />
      </g>
    );
  }

  return (
    <g {...opacityStyle(rest.isFiltered)} pointerEvents="none">
      {rest.labels?.map(({ height, id: labelId, text, width, x, y }) => {
        if (y === undefined || x === undefined) {
          return undefined;
        }

        return (
          <Group
            // Add a tiny bit of height so letters aren't cut off
            height={(height ?? 0) + 2}
            key={labelId}
            left={x}
            top={y}
            width={width}
          >
            <foreignObject height={(height ?? 0) + 2} width={width}>
              <Text>{text}</Text>
            </foreignObject>
          </Group>
        );
      })}
      {(rest.sections ?? []).map((section) => (
        <LinePath
          data={[section.startPoint, ...(section.bendPoints ?? []), section.endPoint]}
          key={section.id}
          stroke={edgeStrokeColor}
          strokeDasharray={rest.isSetupTeardown ? "10,5" : undefined}
          strokeWidth={isSelected ? 3 : 1}
          x={(point: ElkPoint) => point.x}
          y={(point: ElkPoint) => point.y}
        />
      ))}
    </g>
  );
};

export default CustomEdge;
