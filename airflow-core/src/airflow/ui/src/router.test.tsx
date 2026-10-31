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
import { useEffect } from "react";

import { act, render, screen } from "@testing-library/react";
import { Outlet, RouterProvider, createMemoryRouter, matchRoutes, type RouteObject } from "react-router-dom";
import { describe, expect, it } from "vitest";

import { TabEntity, TaskInstanceTab } from "src/constants/tab";
import { getTabPath } from "src/utils/tab";

import { routerConfig, taskInstanceRoutes } from "./router";

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

type StubbableRoute = {
  readonly children?: ReadonlyArray<StubbableRoute>;
  readonly index?: boolean;
  readonly path?: string;
};

const mounts = { count: 0 };

const LayoutProbe = () => {
  useEffect(() => {
    mounts.count += 1;
  }, []);

  return (
    <div data-testid="layout">
      <Outlet />
    </div>
  );
};

const TabContent = () => <div data-testid="tab-content" />;

/** The real route tree with every element replaced, so only the nesting is under test. */
const stubElements = (route: StubbableRoute): RouteObject => {
  if (route.children !== undefined) {
    return { children: route.children.map(stubElements), element: <Outlet />, path: route.path };
  }

  return route.index === true
    ? { element: <TabContent />, index: true }
    : { element: <TabContent />, path: route.path };
};

// One per level and tab the Dag detail screen offers, including the legacy required_actions deep
// links and the plugin routes nested under each level.
const DAG_DETAIL_PATHS = [
  "/dags/example",
  "/dags/example/runs",
  "/dags/example/tasks",
  "/dags/example/calendar",
  "/dags/example/required_actions",
  "/dags/example/backfills",
  "/dags/example/backfills/12",
  "/dags/example/events",
  "/dags/example/code",
  "/dags/example/versions",
  "/dags/example/details",
  "/dags/example/plugin/my_view/nested/42",
  "/dags/example/runs/run_1",
  "/dags/example/runs/run_1/required_actions",
  "/dags/example/runs/run_1/events",
  "/dags/example/runs/run_1/code",
  "/dags/example/runs/run_1/details",
  "/dags/example/runs/run_1/asset_events",
  "/dags/example/runs/run_1/callbacks",
  "/dags/example/runs/run_1/plugin/my_view/nested",
  "/dags/example/runs/run_1/tasks/task_1",
  "/dags/example/runs/run_1/tasks/task_1/logs",
  "/dags/example/runs/run_1/tasks/task_1/required_actions",
  "/dags/example/runs/run_1/tasks/task_1/xcom",
  "/dags/example/runs/run_1/tasks/task_1/task_instances",
  "/dags/example/runs/run_1/tasks/task_1/plugin/my_view",
  "/dags/example/runs/run_1/tasks/task_1/mapped",
  "/dags/example/runs/run_1/tasks/task_1/mapped/details",
  "/dags/example/runs/run_1/tasks/task_1/mapped/0",
  "/dags/example/runs/run_1/tasks/task_1/mapped/0/logs",
  "/dags/example/runs/run_1/tasks/group/group_1",
  "/dags/example/runs/run_1/tasks/group/group_1/iterations",
  "/dags/example/tasks/task_1",
  "/dags/example/tasks/task_1/task_instances",
  "/dags/example/tasks/task_1/required_actions",
  "/dags/example/tasks/task_1/events",
  "/dags/example/tasks/task_1/plugin/my_view",
  "/dags/example/tasks/group/group_1",
  "/dags/example/tasks/group/group_1/task_instances",
  "/dags/example/tasks/group/group_1/required_actions",
];

describe("the Dag detail levels share one mount", () => {
  it("reaches every level and tab without remounting dags/:dagId", async () => {
    const children = routerConfig[0]?.children as ReadonlyArray<StubbableRoute> | undefined;
    const dagRoute = children?.find((route) => route.path === "dags/:dagId");

    if (dagRoute === undefined) {
      throw new Error("no dags/:dagId route to mount the Dag detail screen on");
    }

    const router = createMemoryRouter([{ ...stubElements(dagRoute), element: <LayoutProbe /> }], {
      initialEntries: [DAG_DETAIL_PATHS[0] as string],
    });

    render(<RouterProvider router={router} />);
    expect(mounts.count).toBe(1);
    // The node itself, not just the component: a Grid's scroll offset lives on the element.
    const layout = screen.getByTestId("layout");

    for (const pathname of DAG_DETAIL_PATHS.slice(1)) {
      // eslint-disable-next-line no-await-in-loop
      await act(async () => router.navigate(pathname));

      expect(router.state.location.pathname).toBe(pathname);
      expect(screen.getAllByTestId("tab-content").length).toBeGreaterThan(0);
      expect(screen.getByTestId("layout")).toBe(layout);
    }

    expect(mounts.count).toBe(1);
  });
});
