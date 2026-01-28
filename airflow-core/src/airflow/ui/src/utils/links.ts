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
import type { TaskInstanceResponse } from "openapi/requests/types.gen";

const DAG_RUN_ROUTES = ["required_actions", "asset_events", "events", "code", "details"] as const;
const TASK_ROUTES = ["task_instances", "required_actions", "events"] as const;
const TASK_INSTANCE_ROUTES = [
  "required_actions",
  "rendered_templates",
  "xcom",
  "asset_events",
  "events",
  "code",
  "details",
] as const;

export type DagRunRoutePath = (typeof DAG_RUN_ROUTES)[number];
export type TaskRoutePath = (typeof TASK_ROUTES)[number];
export type TaskInstanceRoutePath = (typeof TASK_INSTANCE_ROUTES)[number];

export const getTaskInstanceLink = (
  tiOrParams:
    | TaskInstanceResponse
    | {
        dagId: string;
        dagRunId: string;
        mapIndex?: number;
        taskId: string;
      },
): string => {
  if ("dag_id" in tiOrParams) {
    return `/dags/${tiOrParams.dag_id}/runs/${tiOrParams.dag_run_id}/tasks/${tiOrParams.task_id}${
      tiOrParams.map_index >= 0 ? `/mapped/${tiOrParams.map_index}` : ""
    }`;
  }

  const { dagId, dagRunId, mapIndex = -1, taskId } = tiOrParams;

  return `/dags/${dagId}/runs/${dagRunId}/tasks/${taskId}${mapIndex >= 0 ? `/mapped/${mapIndex}` : ""}`;
};

export const getRedirectPath = (targetPath: string): string => {
  const baseHref = document.querySelector("head > base")?.getAttribute("href") ?? "";
  const baseUrl = new URL(baseHref, globalThis.location.origin);

  return new URL(targetPath, baseUrl).pathname;
};

const extractSubRoute = (
  pathname: string,
  patterns: Array<RegExp>,
  allowedRoutes: ReadonlyArray<string>,
): string => {
  for (const pattern of patterns) {
    const match = pattern.exec(pathname);
    const subRoute = match?.groups?.subRoute;

    if (subRoute !== undefined && subRoute !== "") {
      if (subRoute.startsWith("plugin/") || allowedRoutes.includes(subRoute)) {
        return `/${subRoute}`;
      }
    }
  }

  return "";
};

export const getDagRunAdditionalPath = (pathname: string): string =>
  extractSubRoute(pathname, [/\/runs\/[^/]+\/(?<subRoute>.+)$/u], DAG_RUN_ROUTES);

export const getTaskAdditionalPath = (pathname: string): string =>
  extractSubRoute(pathname, [/\/dags\/[^/]+\/tasks\/(?:group\/)?[^/]+\/(?<subRoute>.+)$/u], TASK_ROUTES);

export const getTaskInstanceAdditionalPath = (pathname: string): string =>
  extractSubRoute(
    pathname,
    [/\/tasks\/[^/]+\/mapped\/[^/]+\/(?<subRoute>.+)$/u, /\/tasks\/(?:group\/)?[^/]+\/(?<subRoute>.+)$/u],
    TASK_INSTANCE_ROUTES,
  );

export const buildTaskUrl = (params: {
  currentPathname: string;
  dagId: string;
  isGroup?: boolean;
  isMapped?: boolean;
  runId?: string;
  taskId: string;
}): string => {
  const { currentPathname, dagId, isGroup = false, isMapped = false, runId, taskId } = params;

  // Mapped tasks navigate to list view (no tab preservation)
  if (isMapped && runId !== undefined) {
    return `/dags/${dagId}/runs/${runId}/tasks/${taskId}/mapped`;
  }

  const groupPath = isGroup ? "group/" : "";
  const additionalPath = isGroup ? "" : getTaskAdditionalPath(currentPathname);
  const basePath =
    runId === undefined
      ? `/dags/${dagId}/tasks/${groupPath}${taskId}`
      : `/dags/${dagId}/runs/${runId}/tasks/${groupPath}${taskId}`;

  return `${basePath}${additionalPath}`;
};

export const buildDagRunUrl = (params: { currentPathname: string; dagId: string; runId: string }): string => {
  const { currentPathname, dagId, runId } = params;
  const additionalPath = getDagRunAdditionalPath(currentPathname);

  return `/dags/${dagId}/runs/${runId}${additionalPath}`;
};

export const buildTaskInstanceUrl = (params: {
  currentPathname: string;
  dagId: string;
  isGroup?: boolean;
  isMapped?: boolean;
  mapIndex?: string;
  runId: string;
  taskId: string;
}): string => {
  const { currentPathname, dagId, isGroup = false, isMapped = false, mapIndex, runId, taskId } = params;

  const groupPath = isGroup ? "group/" : "";
  const isMappedList = isMapped && (mapIndex === undefined || mapIndex === "-1");

  // Only preserve tabs for individual task instances (not groups or mapped task lists)
  const additionalPath = isGroup || isMappedList ? "" : getTaskInstanceAdditionalPath(currentPathname);

  let basePath = `/dags/${dagId}/runs/${runId}/tasks/${groupPath}${taskId}`;

  if (isMapped) {
    basePath += `/mapped`;
    if (!isMappedList) {
      basePath += `/${mapIndex}`;
    }
  }

  return `${basePath}${additionalPath}`;
};
