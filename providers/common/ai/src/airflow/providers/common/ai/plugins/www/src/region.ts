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

// Shared by both plugin bundles, so it must stay free of top-level side effects (api.ts and
// model-api.ts read `document` at import time). Those two import it as "./region.ts" because
// the node:test runner loads them without Vite and cannot resolve the "src/" alias.

export type TaskRegion = { region_id: string; region_index: number };

/** The slice of the host's TaskInstanceResponse the plugins read. */
export type HostTaskInstance = { id?: string; region_id?: string; region_index?: number };

/**
 * Whether a host that passes `taskInstance` has not resolved it yet. Reading before it
 * resolves would address a loop task without its pass; a host that predates `taskInstance`
 * never passes the key and keeps the map-only requests.
 */
export function isSelectionPending(props: { taskInstance?: HostTaskInstance }): boolean {
  return "taskInstance" in props && props.taskInstance == null;
}

/** The loop pass a task instance belongs to; the host omits both fields outside a loop. */
export function regionOf(taskInstance?: HostTaskInstance): TaskRegion | undefined {
  return taskInstance?.region_id !== undefined && taskInstance.region_index !== undefined
    ? { region_id: taskInstance.region_id, region_index: taskInstance.region_index }
    : undefined;
}

export function regionQuery(region?: TaskRegion): string {
  return region === undefined
    ? ""
    : `&region_id=${encodeURIComponent(region.region_id)}&region_index=${region.region_index}`;
}
