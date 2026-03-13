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

// Grid layout constants - shared between Grid and Gantt for alignment
export const ROW_HEIGHT = 20;
export const GRID_OUTER_PADDING_PX = 64; // pt={16} = 16 * 4 = 64px
export const GRID_HEADER_PADDING_PX = 16; // pt={4} = 4 * 4 = 16px
export const GRID_HEADER_HEIGHT_PX = 100; // height="100px" for duration bars

// Gantt chart's x-axis height (time labels at top of chart)
export const GANTT_AXIS_HEIGHT_PX = 36;

// Total offset from top of Grid component to where task rows begin,
// minus the Gantt axis height since the chart includes its own top axis
export const GRID_BODY_OFFSET_PX =
  GRID_OUTER_PADDING_PX + GRID_HEADER_PADDING_PX + GRID_HEADER_HEIGHT_PX - GANTT_AXIS_HEIGHT_PX;

// Version indicator constants
export const BAR_HEIGHT = GRID_HEADER_HEIGHT_PX; // Duration bar height matches grid header
export const BUNDLE_VERSION_INDICATOR_TOP = 93; // Position from top for bundle version icon
export const BUNDLE_VERSION_INDICATOR_LEFT = -2; // Position from left for bundle version icon
export const BUNDLE_VERSION_ICON_SIZE = 15; // Size of the git commit icon
export const DAG_VERSION_INDICATOR_HEIGHT = 104; // Height of the vertical line indicator
export const VERSION_INDICATOR_Z_INDEX = 1; // Z-index for version indicators
