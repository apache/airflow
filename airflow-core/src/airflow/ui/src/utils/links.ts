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

const SAFE_EXTERNAL_URL_SCHEMES = new Set(["http:", "https:", "mailto:"]);

/**
 * Pass-through filter for href values that originate outside the application —
 * for example DAG-author-supplied `owner_links`, or operator extra-link URLs
 * read from task-pushed XCom.
 *
 * Returns the URL unchanged when it is either a same-origin / relative path or
 * uses one of the allow-listed schemes (`http:`, `https:`, `mailto:`).
 * Returns `undefined` for any other scheme (`javascript:`, `data:`, `file:`,
 * `vbscript:`, etc.) and for unparsable input, matching the scheme-allowlist
 * policy already applied to markdown links via react-markdown's default
 * `urlTransform` and to log / XCom linkification (which is `https?://`-only).
 *
 * Callers should fall back to plain text or skip rendering when this returns
 * `undefined`.
 */
export const getSafeExternalUrl = (url: string): string | undefined => {
  const trimmed = url.trim();

  if (trimmed === "") {
    return undefined;
  }

  let parsed: URL;

  try {
    parsed = new URL(trimmed, globalThis.location.origin);
  } catch {
    return undefined;
  }

  // Same-origin URL (relative input, or absolute pointing at our own origin).
  // We have to compare against `location.origin` rather than just looking at
  // the protocol because `new URL("/foo", origin)` produces a URL whose
  // protocol is the origin's protocol (typically `http(s):`), so a
  // protocol-only check would let through any non-allow-listed scheme that
  // happens to share the origin's protocol shape.
  if (parsed.origin === globalThis.location.origin) {
    return trimmed;
  }

  if (SAFE_EXTERNAL_URL_SCHEMES.has(parsed.protocol)) {
    return trimmed;
  }

  return undefined;
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
  const additionalPath =
    isGroup || (isMapped && (mapIndex === undefined || mapIndex === "-1"))
      ? ""
      : getTaskInstanceAdditionalPath(currentPathname);

  let basePath = `/dags/${dagId}/runs/${runId}/tasks/${groupPath}${taskId}`;

  if (isMapped && !isGroup) {
    basePath += `/mapped`;
    if (mapIndex !== undefined && mapIndex !== "-1") {
      basePath += `/${mapIndex}`;
    }
  }

  return `${basePath}${additionalPath}`;
};
