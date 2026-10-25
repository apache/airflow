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
import { matchRoutes, type LazyRouteFunction, type RouteObject } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { TabEntity, TaskInstanceTab } from "src/constants/tab";
import { getTabPath } from "src/utils/tab";

import { loadRouteModule, routerConfig, taskInstanceRoutes } from "./router";

vi.mock("src/pages/DagsList", () => ({ DagsList: () => null }));

const getDescendantRoutes = (routes: ReadonlyArray<RouteObject>): Array<RouteObject> =>
  routes.flatMap((route) => [route, ...getDescendantRoutes(route.children ?? [])]);

const getAdditionalPath = (pathname: string) => {
  const matches = matchRoutes(routerConfig, pathname) ?? [];

  return getTabPath(
    matches.map((match) => ({
      handle: "handle" in match.route ? match.route.handle : undefined,
    })),
    TabEntity.Dag,
  );
};

describe("Dag route handles", () => {
  it.each(["runs", "tasks", "calendar", "backfills", "events", "code", "details"])(
    "preserves the %s Dag tab",
    (tab) => {
      expect(getAdditionalPath(`/dags/example/${tab}`)).toBe(`/${tab}`);
    },
  );

  it("does not preserve a plugin route when destination compatibility is unknown", () => {
    expect(getAdditionalPath("/dags/example/plugin/test/nested/detail/42")).toBe("");
  });

  it.each([
    "/assets/1",
    "/dags/example/required_actions",
    "/dags/example/backfills/12",
    "/dags/example/runs/run_1",
    "/dags/example/tasks/task_1",
    "/dags/example/tasks/group/group_1",
    "/dags/example/runs/run_1/tasks/task_1/details",
    "/dags/example/unknown",
  ])("does not preserve a tab from %s", (pathname) => {
    expect(getAdditionalPath(pathname)).toBe("");
  });
});

describe("lazy routes", () => {
  beforeEach(() => globalThis.sessionStorage.clear());

  afterEach(() => {
    vi.restoreAllMocks();
    globalThis.sessionStorage.clear();
  });

  it("lazy-loads every page route", () => {
    const [rootRoute] = routerConfig as Array<RouteObject>;

    if (rootRoute === undefined) {
      throw new TypeError("Expected a root route");
    }

    const pageRoutes = getDescendantRoutes(rootRoute.children ?? []).filter((route) => route.path !== "*");

    expect(pageRoutes).not.toHaveLength(0);
    expect(pageRoutes.every((route) => route.element === undefined && typeof route.lazy === "function")).toBe(
      true,
    );
  });

  it("loads a page component on demand", async () => {
    const route = matchRoutes(routerConfig as Array<RouteObject>, "/dags")?.at(-1)?.route;

    expect(route).not.toHaveProperty("element");

    if (typeof route?.lazy !== "function") {
      throw new TypeError("Expected the Dags list route to be lazy");
    }

    const loadRoute = route.lazy as LazyRouteFunction<RouteObject>;
    const [{ Component }, { DagsList }] = await Promise.all([loadRoute(), import("src/pages/DagsList")]);

    expect(Component).toBe(DagsList);
  });

  it("reloads only once when route module loading fails repeatedly", async () => {
    const error = new Error("Route module unavailable");
    const reload = vi.spyOn(globalThis.location, "reload").mockImplementation(() => undefined);

    await expect(loadRouteModule(() => Promise.reject(error))).rejects.toBe(error);
    await expect(loadRouteModule(() => Promise.reject(error))).rejects.toBe(error);

    expect(reload).toHaveBeenCalledOnce();
  });
});

type RouteLike = { readonly children?: ReadonlyArray<RouteLike>; readonly path?: string };

describe("taskInstanceRoutes stay in sync with the TaskInstanceTab enum", () => {
  it("uses exactly the enum values as its named tab paths", () => {
    const namedPaths = (taskInstanceRoutes as ReadonlyArray<RouteLike>)
      .flatMap((route) => route.children ?? [route])
      .map((route) => route.path)
      .filter((path): path is string => path !== undefined && !path.startsWith("plugin/"));

    expect(namedPaths.sort()).toStrictEqual(Object.values(TaskInstanceTab).sort());
  });
});
