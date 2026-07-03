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

type Point = {
  x: number;
  y: number;
};

export type ManualEdgeNode = {
  height?: number;
  id: string;
  position: Point;
  width?: number;
};

const isSamePoint = (firstPoint: Point, secondPoint: Point) =>
  firstPoint.x === secondPoint.x && firstPoint.y === secondPoint.y;

const hasNodeDimensions = (
  node: ManualEdgeNode | undefined,
): node is { height: number; width: number } & ManualEdgeNode =>
  node?.height !== undefined && node.width !== undefined;

const getNodeCenter = ({ height, position, width }: { height: number; width: number } & ManualEdgeNode) => ({
  x: position.x + width / 2,
  y: position.y + height / 2,
});

const getSideAttachmentPoint = ({
  fallback,
  node,
  reference,
}: {
  fallback: Point;
  node?: ManualEdgeNode;
  reference: Point;
}) => {
  if (!hasNodeDimensions(node)) {
    return fallback;
  }

  const center = getNodeCenter(node);
  const deltaX = reference.x - center.x;
  const deltaY = reference.y - center.y;

  if (Math.abs(deltaX) > Math.abs(deltaY)) {
    return {
      x: deltaX > 0 ? node.position.x + node.width : node.position.x,
      y: center.y,
    };
  }

  return {
    x: center.x,
    y: deltaY > 0 ? node.position.y + node.height : node.position.y,
  };
};

const getManualEndpointPoints = ({
  source,
  sourceNode,
  target,
  targetNode,
}: {
  source: Point;
  sourceNode?: ManualEdgeNode;
  target: Point;
  targetNode?: ManualEdgeNode;
}) => {
  const sourceReference = hasNodeDimensions(targetNode) ? getNodeCenter(targetNode) : target;
  const targetReference = hasNodeDimensions(sourceNode) ? getNodeCenter(sourceNode) : source;

  return {
    source: getSideAttachmentPoint({ fallback: source, node: sourceNode, reference: sourceReference }),
    target: getSideAttachmentPoint({ fallback: target, node: targetNode, reference: targetReference }),
  };
};

const getCleanPath = (points: Array<Point>) =>
  points.filter((point, index) => index === 0 || !isSamePoint(point, points[index - 1] ?? point));

const getPathData = (points: Array<Point>) => {
  const [firstPoint, ...rest] = getCleanPath(points);

  if (firstPoint === undefined) {
    return "";
  }

  return `M ${firstPoint.x} ${firstPoint.y}${rest.map((point) => ` L ${point.x} ${point.y}`).join("")}`;
};

const getSimpleOrthogonalPathPoints = (source: Point, target: Point) => {
  if (source.x === target.x || source.y === target.y) {
    return [source, target];
  }

  const middleX = (source.x + target.x) / 2;

  return [source, { x: middleX, y: source.y }, { x: middleX, y: target.y }, target];
};

export const getManualEdgePath = ({
  source,
  sourceNode,
  target,
  targetNode,
}: {
  source: Point;
  sourceNode?: ManualEdgeNode;
  target: Point;
  targetNode?: ManualEdgeNode;
}) => {
  const endpoints = getManualEndpointPoints({ source, sourceNode, target, targetNode });

  return getPathData(getSimpleOrthogonalPathPoints(endpoints.source, endpoints.target));
};
