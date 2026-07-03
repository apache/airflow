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
import type { GraphNode } from "./manualLayout";

export type ManualLayoutViewport = {
  readonly x: number;
  readonly y: number;
  readonly zoom: number;
};

export type ManualLayoutState = {
  readonly isManualLayout: boolean;
  readonly manualEdgeNodeIds: Set<string>;
  readonly manualNodes: Array<GraphNode>;
  readonly viewport?: ManualLayoutViewport;
};

const keySeparator = "\0";
const manualLayoutStates = new Map<string, ManualLayoutState>();
const pendingClearTimers = new Map<string, ReturnType<typeof setTimeout>>();

export const getManualLayoutKey = ({ dagId, runId = "" }: { dagId: string; runId?: string }) =>
  `${dagId}${keySeparator}${runId}`;

const cloneManualLayoutState = (state: ManualLayoutState): ManualLayoutState => ({
  isManualLayout: state.isManualLayout,
  manualEdgeNodeIds: new Set(state.manualEdgeNodeIds),
  manualNodes: [...state.manualNodes],
  viewport: state.viewport === undefined ? undefined : { ...state.viewport },
});

export const getManualLayoutState = (key: string) => {
  const state = manualLayoutStates.get(key);

  return state === undefined ? undefined : cloneManualLayoutState(state);
};

export const setManualLayoutState = (key: string, state: ManualLayoutState) => {
  manualLayoutStates.set(key, cloneManualLayoutState(state));
};

export const setManualLayoutViewport = (key: string, viewport: ManualLayoutViewport) => {
  const state = manualLayoutStates.get(key);

  if (state !== undefined) {
    manualLayoutStates.set(key, cloneManualLayoutState({ ...state, viewport }));
  }
};

export const clearManualLayoutState = (key: string) => {
  manualLayoutStates.delete(key);
};

export const clearManualLayoutStatesForDag = (dagId: string) => {
  const pendingClearTimer = pendingClearTimers.get(dagId);

  if (pendingClearTimer !== undefined) {
    clearTimeout(pendingClearTimer);
    pendingClearTimers.delete(dagId);
  }

  const prefix = `${dagId}${keySeparator}`;

  for (const key of manualLayoutStates.keys()) {
    if (key.startsWith(prefix)) {
      manualLayoutStates.delete(key);
    }
  }
};

export const scheduleClearManualLayoutStatesForDag = (dagId: string) => {
  if (pendingClearTimers.has(dagId)) {
    return;
  }

  pendingClearTimers.set(
    dagId,
    setTimeout(() => {
      clearManualLayoutStatesForDag(dagId);
    }),
  );
};

export const cancelClearManualLayoutStatesForDag = (dagId: string) => {
  const pendingClearTimer = pendingClearTimers.get(dagId);

  if (pendingClearTimer !== undefined) {
    clearTimeout(pendingClearTimer);
    pendingClearTimers.delete(dagId);
  }
};

export const clearAllManualLayoutStates = () => {
  for (const pendingClearTimer of pendingClearTimers.values()) {
    clearTimeout(pendingClearTimer);
  }

  pendingClearTimers.clear();
  manualLayoutStates.clear();
};
