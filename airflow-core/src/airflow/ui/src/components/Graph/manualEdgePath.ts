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

type Rect = {
  bottom: number;
  left: number;
  right: number;
  top: number;
};

const nodePadding = 12;
const maxAxisCandidates = 24;
const bendPenalty = 16;

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

const getNodeRect = (node: ManualEdgeNode): Rect | undefined => {
  if (node.height === undefined || node.width === undefined) {
    return undefined;
  }

  return {
    bottom: node.position.y + node.height + nodePadding,
    left: node.position.x - nodePadding,
    right: node.position.x + node.width + nodePadding,
    top: node.position.y - nodePadding,
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

const rangesOverlap = ({
  firstEnd,
  firstStart,
  secondEnd,
  secondStart,
}: {
  firstEnd: number;
  firstStart: number;
  secondEnd: number;
  secondStart: number;
}) =>
  Math.max(Math.min(firstStart, firstEnd), secondStart) < Math.min(Math.max(firstStart, firstEnd), secondEnd);

const horizontalSegmentIntersectsRect = (start: Point, end: Point, rect: Rect) =>
  start.y > rect.top &&
  start.y < rect.bottom &&
  rangesOverlap({ firstEnd: end.x, firstStart: start.x, secondEnd: rect.right, secondStart: rect.left });

const verticalSegmentIntersectsRect = (start: Point, end: Point, rect: Rect) =>
  start.x > rect.left &&
  start.x < rect.right &&
  rangesOverlap({ firstEnd: end.y, firstStart: start.y, secondEnd: rect.bottom, secondStart: rect.top });

const segmentIntersectsRect = (start: Point, end: Point, rect: Rect) => {
  if (start.x === end.x) {
    return verticalSegmentIntersectsRect(start, end, rect);
  }

  if (start.y === end.y) {
    return horizontalSegmentIntersectsRect(start, end, rect);
  }

  return false;
};

const pathIntersectsAnyRect = (points: Array<Point>, rects: Array<Rect>) => {
  const cleanPath = getCleanPath(points);

  return cleanPath.some((point, index) => {
    const nextPoint = cleanPath[index + 1];

    return nextPoint === undefined
      ? false
      : rects.some((rect) => segmentIntersectsRect(point, nextPoint, rect));
  });
};

const getPathLength = (points: Array<Point>) => {
  const cleanPath = getCleanPath(points);

  return cleanPath.reduce((length, point, index) => {
    const nextPoint = cleanPath[index + 1];

    return nextPoint === undefined
      ? length
      : length + Math.abs(point.x - nextPoint.x) + Math.abs(point.y - nextPoint.y);
  }, 0);
};

const getBendCount = (points: Array<Point>) => {
  const cleanPath = getCleanPath(points);

  return cleanPath.reduce((bendCount, point, index) => {
    const previousPoint = cleanPath[index - 1];
    const nextPoint = cleanPath[index + 1];

    if (previousPoint === undefined || nextPoint === undefined) {
      return bendCount;
    }

    const previousDirection = previousPoint.x === point.x ? "vertical" : "horizontal";
    const nextDirection = point.x === nextPoint.x ? "vertical" : "horizontal";

    return previousDirection === nextDirection ? bendCount : bendCount + 1;
  }, 0);
};

const getPathScore = (points: Array<Point>) => getPathLength(points) + getBendCount(points) * bendPenalty;

const getLimitedAxisCandidates = (values: Array<number>, preferredValue: number) =>
  [...new Set(values)]
    .sort(
      (firstValue, secondValue) =>
        Math.abs(firstValue - preferredValue) - Math.abs(secondValue - preferredValue),
    )
    .slice(0, maxAxisCandidates);

const getCandidatePaths = ({
  rects,
  source,
  target,
}: {
  rects: Array<Rect>;
  source: Point;
  target: Point;
}) => {
  const middleX = (source.x + target.x) / 2;
  const middleY = (source.y + target.y) / 2;
  const leftBoundary = Math.min(source.x, target.x, ...rects.map((rect) => rect.left)) - nodePadding;
  const rightBoundary = Math.max(source.x, target.x, ...rects.map((rect) => rect.right)) + nodePadding;
  const topBoundary = Math.min(source.y, target.y, ...rects.map((rect) => rect.top)) - nodePadding;
  const bottomBoundary = Math.max(source.y, target.y, ...rects.map((rect) => rect.bottom)) + nodePadding;
  const xCandidates = getLimitedAxisCandidates(
    [
      source.x,
      target.x,
      middleX,
      leftBoundary,
      rightBoundary,
      ...rects.flatMap((rect) => [rect.left, rect.right]),
    ],
    middleX,
  );
  const yCandidates = getLimitedAxisCandidates(
    [
      source.y,
      target.y,
      middleY,
      topBoundary,
      bottomBoundary,
      ...rects.flatMap((rect) => [rect.top, rect.bottom]),
    ],
    middleY,
  );
  const paths = [
    [source, { x: target.x, y: source.y }, target],
    [source, { x: source.x, y: target.y }, target],
  ];

  for (const x of xCandidates) {
    paths.push([source, { x, y: source.y }, { x, y: target.y }, target]);
  }

  for (const y of yCandidates) {
    paths.push([source, { x: source.x, y }, { x: target.x, y }, target]);
  }

  for (const x of xCandidates) {
    for (const y of yCandidates) {
      paths.push([source, { x, y: source.y }, { x, y }, { x: target.x, y }, target]);
      paths.push([source, { x: source.x, y }, { x, y }, { x, y: target.y }, target]);
    }
  }

  return paths;
};

export const getManualEdgePath = ({
  nodes,
  source,
  sourceNode,
  target,
  targetNode,
}: {
  nodes: Array<ManualEdgeNode>;
  source: Point;
  sourceNode?: ManualEdgeNode;
  target: Point;
  targetNode?: ManualEdgeNode;
}) => {
  const endpoints = getManualEndpointPoints({ source, sourceNode, target, targetNode });
  const rects = nodes.flatMap((node) => {
    const rect = getNodeRect(node);

    return rect === undefined ? [] : [rect];
  });
  const paths = getCandidatePaths({ rects, source: endpoints.source, target: endpoints.target });
  const [clearPath] = paths
    .filter((path) => !pathIntersectsAnyRect(path, rects))
    .sort((firstPath, secondPath) => getPathScore(firstPath) - getPathScore(secondPath));

  return getPathData(clearPath ?? paths[0] ?? [endpoints.source, endpoints.target]);
};
