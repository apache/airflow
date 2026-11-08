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
import { matchRoutes } from "react-router-dom";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";

import { basePath as uiBasePath } from "src/basePath";
import {
  DEFAULT_TASK_INSTANCE_TAB_PATHS,
  type DefaultTaskInstanceTab,
  type TaskInstanceTabValue,
} from "src/constants/tab";
import { routerConfig, taskInstanceRoutes } from "src/router";

// Links address the current execution at these coordinates; a try_number would pin the page to one attempt.
export const getTaskInstanceLink = (
  tiOrParams:
    | Pick<
        TaskInstanceResponse,
        "dag_id" | "dag_run_id" | "map_index" | "region_id" | "region_index" | "task_id"
      >
    | {
        dagId: string;
        dagRunId: string;
        mapIndex?: number;
        taskId: string;
      },
  tab?: TaskInstanceTabValue,
): string => {
  const tabPath = tab === undefined ? "" : `/${tab}`;

  if ("dag_id" in tiOrParams) {
    const path = `/dags/${tiOrParams.dag_id}/runs/${tiOrParams.dag_run_id}/tasks/${tiOrParams.task_id}${
      tiOrParams.map_index >= 0 ? `/mapped/${tiOrParams.map_index}` : ""
    }${tabPath}`;

    if (tiOrParams.region_id === undefined) {
      return path;
    }
    const query = new URLSearchParams({
      region_id: tiOrParams.region_id,
      region_index: String(tiOrParams.region_index),
    });

    return `${path}?${query}`;
  }

  const { dagId, dagRunId, mapIndex = -1, taskId } = tiOrParams;

  return `/dags/${dagId}/runs/${dagRunId}/tasks/${taskId}${
    mapIndex >= 0 ? `/mapped/${mapIndex}` : ""
  }${tabPath}`;
};

export const getRedirectPath = (targetPath: string): string => {
  const baseHref = document.querySelector("head > base")?.getAttribute("href") ?? "";
  const baseUrl = new URL(baseHref, globalThis.location.origin);

  return new URL(targetPath, baseUrl).pathname;
};

// Build a same-origin "next" target (path + query + hash) from a Location.
// Using a relative URL ensures redirects work correctly when the UI is
// reached through a proxy or a different origin than the API server reports
// (e.g. Gitpod port-based domains, see #46533).
export const getNextHref = (location: Pick<Location, "hash" | "pathname" | "search">): string =>
  `${location.pathname}${location.search}${location.hash}`;

export const getTaskInstanceAdditionalPath = (pathname: string): string => {
  const subRoutes = taskInstanceRoutes.flatMap((route) => (route.path === undefined ? [] : [route.path]));
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

// Resolve a stored default-tab preference to a route path. Logs maps to "" (the index
// route), and any unknown value falls back to "" so the index keeps rendering in place.
export const getDefaultTaskInstanceTabPath = (value: unknown): string =>
  typeof value === "string" && value in DEFAULT_TASK_INSTANCE_TAB_PATHS
    ? DEFAULT_TASK_INSTANCE_TAB_PATHS[value as DefaultTaskInstanceTab]
    : "";

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

  return SAFE_EXTERNAL_URL_SCHEMES.has(parsed.protocol) ? trimmed : undefined;
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

/**
 * Where a plugin link should open: in this tab for the UI's own origin, a new one otherwise.
 *
 * A URL that cannot be parsed is treated as external, so a malformed plugin link never navigates the
 * app away from the page the user is on.
 */
export const getLinkTarget = (url: string): "_blank" | "_self" => {
  try {
    return new URL(url, globalThis.location.origin).origin === globalThis.location.origin
      ? "_self"
      : "_blank";
  } catch {
    return "_blank";
  }
};

const decodeSegments = (path: string) =>
  path.split("/").map((segment) => {
    try {
      return decodeURIComponent(segment);
    } catch {
      // A malformed escape is not decodable; compare it as written.
      return segment;
    }
  });

/**
 * Whether two paths address the same page, compared segment by segment once decoded.
 *
 * A link action's identifiers arrive percent-encoded from `applyPluginTemplate`, while the
 * router's own `location.pathname` carries them raw -- a run id like
 * `manual__2026-10-20T12:00:00+00:00` is `...T12%3A00%3A00%2B00%3A00` on one side and keeps its
 * `:` and `+` on the other. Comparing the strings would call every run-scoped page a different
 * page. Segments are decoded individually so an encoded `/` stays a value rather than becoming
 * a boundary.
 */
const isSamePath = (left: string, right: string): boolean => {
  const leftSegments = decodeSegments(left);
  const rightSegments = decodeSegments(right);

  return (
    leftSegments.length === rightSegments.length &&
    leftSegments.every((segment, index) => segment === rightSegments[index])
  );
};

/**
 * The query string a link should navigate with, keeping the current one when it stays put.
 *
 * A key the link names replaces every value the page had for it, rather than adding to them,
 * so a link to `?state=failed` means failed rather than "failed as well as whatever was
 * already selected" -- while a link that repeats a key keeps all of its own values.
 */
const mergeSearch = (
  pathname: string,
  linkParams: URLSearchParams,
  currentLocation?: { pathname: string; search: string },
): string => {
  if (currentLocation === undefined || !isSamePath(currentLocation.pathname, pathname)) {
    return linkParams.toString() === "" ? "" : `?${linkParams}`;
  }

  const merged = new URLSearchParams(currentLocation.search);

  for (const key of new Set(linkParams.keys())) {
    merged.delete(key);
  }
  for (const [key, value] of linkParams) {
    merged.append(key, value);
  }

  return merged.toString() === "" ? "" : `?${merged}`;
};

/**
 * Resolve `url` to a path this app's router owns, or `undefined` if it is not one.
 *
 * Matched against the router's own route table rather than just the base path, because a
 * same-origin URL is not necessarily a UI page -- `/api/v2/...` and a plugin's own FastAPI
 * pages sit under the same prefix and must be followed as ordinary links. The table ends in a
 * catch-all that renders the error page, so landing on that leaf means "no UI route for this
 * path", not "internal".
 *
 * The result is relative to the base path, ready to hand to a router link, which adds the
 * prefix back itself.
 *
 * Pass `currentLocation` to keep the query string the user already has when the link stays on
 * the same page. A link's target is fixed when the plugin loads, so without this a button on
 * the Dags list that only wants to add one parameter would navigate with its own query string
 * and silently drop every filter, search term and sort the user had set. Keys the link names
 * win; the rest survive.
 *
 * Only when the path is unchanged: across pages the parameters belong to the page being left
 * -- carrying a Dag Runs `?state=failed` onto Task Instances would filter something the user
 * never asked to filter.
 *
 * Both a relative href and a root-absolute one reach the right page on a deployment served
 * under a base path, so a plugin does not have to know what that path is to link inside the UI.
 */
export const getInternalRouteTo = (
  url: string,
  currentLocation?: { pathname: string; search: string },
): string | undefined => {
  let parsed: URL;

  try {
    // Resolved against `<base href>` rather than the bare origin, so a relative href such as
    // `dags/{DAG_ID}/runs` lands under the deployment's base path -- the same place the browser
    // would take it -- instead of at the root, where no route matches.
    //
    // An href that is only a query or a fragment names no page at all, and a browser resolves
    // it against the current document rather than `<base>`, so it is resolved that way here
    // too: otherwise `?tab=graph` would navigate to the deployment root.
    const pathless = url.startsWith("?") || url.startsWith("#");

    if (pathless && currentLocation === undefined) {
      return undefined;
    }

    parsed = new URL(
      url,
      pathless
        ? new URL(`${uiBasePath}${currentLocation?.pathname ?? "/"}`, document.baseURI)
        : document.baseURI,
    );
  } catch {
    return undefined;
  }

  if (parsed.origin !== globalThis.location.origin) {
    return undefined;
  }

  const matches = (candidate: string) => {
    const matched = matchRoutes(routerConfig, candidate, uiBasePath || "/");

    return matched !== null && matched.at(-1)?.route.path !== "*";
  };

  // A root-absolute href is written without knowing the deployment's base path, so try it under
  // the base path before giving up. Otherwise `/dags/x/runs` on a `/airflow` deployment would
  // open `host/dags/x/runs` in a new tab -- a blank page, not the Dag it names.
  let absolute = parsed.pathname;

  if (!matches(absolute) && uiBasePath !== "" && !absolute.startsWith(`${uiBasePath}/`)) {
    const underBase = `${uiBasePath}${absolute}`;

    if (matches(underBase)) {
      absolute = underBase;
    }
  }

  if (!matches(absolute)) {
    return undefined;
  }

  const relative =
    uiBasePath !== "" && absolute.startsWith(uiBasePath) ? absolute.slice(uiBasePath.length) : absolute;
  const pathname = relative === "" ? "/" : relative;

  return `${pathname}${mergeSearch(pathname, parsed.searchParams, currentLocation)}${parsed.hash}`;
};
