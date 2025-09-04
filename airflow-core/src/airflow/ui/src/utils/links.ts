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
import { taskInstanceRoutes } from "src/router";

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

export const getTaskInstanceAdditionalPath = (pathname: string): string => {
  const subRoutes = taskInstanceRoutes.filter((route) => route.path !== undefined).map((route) => route.path);
  // Look for patterns like /tasks/{taskId}/mapped/{mapIndex}/{sub-route}
  const mappedRegex = /\/tasks\/[^/]+\/mapped\/[^/]+\/(?<subRoute>.+)$/u;
  const mappedMatch = mappedRegex.exec(pathname);

  if (mappedMatch?.groups?.subRoute !== undefined) {
    return `/${mappedMatch.groups.subRoute}`;
  }

  // Look for patterns like /tasks/{taskId}/{sub-route} or /tasks/group/{groupId}/{sub-route}
  const taskRegex = /\/tasks\/(?:group\/)?[^/]+\/(?<subRoute>.+)$/u;
  const taskMatch = taskRegex.exec(pathname);

  if (taskMatch?.groups?.subRoute !== undefined) {
    const { subRoute } = taskMatch.groups;

    // Only preserve if it's a known task instance route or plugin route
    if (subRoutes.includes(subRoute) || subRoute.startsWith("plugin/")) {
      return `/${subRoute}`;
    }
  }

  return "";
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
  const additionalPath = getTaskInstanceAdditionalPath(currentPathname);

  let basePath = `/dags/${dagId}/runs/${runId}/tasks/${groupPath}${taskId}`;

  if (isMapped) {
    basePath += `/mapped`;
    if (mapIndex !== undefined && mapIndex !== "-1") {
      basePath += `/${mapIndex}`;
    }
  }

  return `${basePath}${additionalPath}`;
};
