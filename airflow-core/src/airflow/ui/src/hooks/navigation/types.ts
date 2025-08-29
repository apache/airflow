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
import type { GridRunsResponse } from "openapi/requests";
import type { GridTask } from "src/layouts/Details/Grid/utils";

export type NavigationMode = "run" | "task" | "TI";

export type ArrowKey = "ArrowDown" | "ArrowLeft" | "ArrowRight" | "ArrowUp";

export type NavigationDirection = "down" | "left" | "right" | "up";

export type NavigationIndices = {
  runIndex: number;
  taskIndex: number;
};

export type UseNavigationProps = {
  onToggleGroup?: (taskId: string) => void;
  runs: Array<GridRunsResponse>;
  tasks: Array<GridTask>;
};

export type UseNavigationReturn = {
  currentIndices: NavigationIndices;
  currentTask: GridTask | undefined;
  handleNavigation: (direction: NavigationDirection) => void;
  mode: NavigationMode;
  setMode: (mode: NavigationMode) => void;
};
