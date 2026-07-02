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

export const testSearchGridSize = 24;
export const testMaxSearchRadius = 24;
export const testRadialDirections = [
  { x: 1, y: 0 },
  { x: -1, y: 0 },
  { x: 0, y: 1 },
  { x: 0, y: -1 },
  { x: 1, y: 1 },
  { x: 1, y: -1 },
  { x: -1, y: 1 },
  { x: -1, y: -1 },
];

export const nodesOverlap = ({
  first,
  second,
}: {
  first: { height?: number; position: { x: number; y: number }; width?: number };
  second: { height?: number; position: { x: number; y: number }; width?: number };
}) =>
  first.position.x < second.position.x + (second.width ?? 0) &&
  first.position.x + (first.width ?? 0) > second.position.x &&
  first.position.y < second.position.y + (second.height ?? 0) &&
  first.position.y + (first.height ?? 0) > second.position.y;
