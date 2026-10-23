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
import { afterEach, describe, it, expect, vi } from "vitest";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";

import {
  buildTaskInstanceUrl,
  getDefaultTaskInstanceTabPath,
  getInternalRouteTo,
  getLinkTarget,
  getNextHref,
  getSafeExternalUrl,
  getTaskInstanceAdditionalPath,
  getTaskInstanceLink,
} from "./links";

describe("getTaskInstanceLink", () => {
  it("links a regional task DTO by exact region without pinning its try", () => {
    const task = {
      dag_id: "dag",
      dag_run_id: "run",
      map_index: -1,
      region_id: "11111111-1111-4111-8111-111111111111",
      region_index: 3,
      task_id: "body.work",
      try_number: 2,
    } as TaskInstanceResponse;

    expect(getTaskInstanceLink(task, "logs")).toBe(
      "/dags/dag/runs/run/tasks/body.work/logs?region_id=11111111-1111-4111-8111-111111111111&region_index=3",
    );
  });
  const testCases = [
    // Individual parameters tests
    {
      description: "individual parameters without map index",
      expected: "/dags/my_dag/runs/run_123/tasks/task_1",
      input: { dagId: "my_dag", dagRunId: "run_123", mapIndex: -1, taskId: "task_1" },
    },
    {
      description: "individual parameters with map index",
      expected: "/dags/my_dag/runs/run_123/tasks/task_1/mapped/5",
      input: { dagId: "my_dag", dagRunId: "run_123", mapIndex: 5, taskId: "task_1" },
    },
    {
      description: "individual parameters with map index of 0",
      expected: "/dags/test_dag/runs/test_run/tasks/mapped_task/mapped/0",
      input: { dagId: "test_dag", dagRunId: "test_run", mapIndex: 0, taskId: "mapped_task" },
    },
    {
      description: "individual parameters without mapIndex property (defaults to -1)",
      expected: "/dags/my_dag/runs/run_123/tasks/task_1",
      input: { dagId: "my_dag", dagRunId: "run_123", taskId: "task_1" },
    },
    // TaskInstanceResponse object tests
    {
      description: "TaskInstanceResponse object without map index",
      expected: "/dags/my_dag/runs/run_123/tasks/task_1",
      input: {
        dag_id: "my_dag",
        dag_run_id: "run_123",
        map_index: -1,
        task_id: "task_1",
      } as TaskInstanceResponse,
    },
    {
      description: "TaskInstanceResponse object with map index",
      expected: "/dags/my_dag/runs/run_123/tasks/task_1/mapped/5",
      input: {
        dag_id: "my_dag",
        dag_run_id: "run_123",
        map_index: 5,
        task_id: "task_1",
      } as TaskInstanceResponse,
    },
    {
      description: "TaskInstanceResponse object with map index of 0",
      expected: "/dags/test_dag/runs/test_run/tasks/mapped_task/mapped/0",
      input: {
        dag_id: "test_dag",
        dag_run_id: "test_run",
        map_index: 0,
        task_id: "mapped_task",
      } as TaskInstanceResponse,
    },
  ];

  it.each(testCases)("should handle $description", ({ expected, input }) => {
    const result = getTaskInstanceLink(input);

    expect(result).toBe(expected);
  });
});

describe("getTaskInstanceAdditionalPath", () => {
  it("should return empty string for basic task path", () => {
    const result = getTaskInstanceAdditionalPath("/dags/my_dag/runs/run_1/tasks/task_1");

    expect(result).toBe("");
  });

  it("should extract sub-route from regular task path", () => {
    const result = getTaskInstanceAdditionalPath("/dags/my_dag/runs/run_1/tasks/task_1/details");

    expect(result).toBe("/details");
  });

  it("should extract sub-route from group task path", () => {
    const result = getTaskInstanceAdditionalPath("/dags/my_dag/runs/run_1/tasks/group/my_group/xcom");

    expect(result).toBe("/xcom");
  });

  it("should extract sub-route from mapped task with index", () => {
    const result = getTaskInstanceAdditionalPath("/dags/my_dag/runs/run_1/tasks/task_1/mapped/5/events");

    expect(result).toBe("/events");
  });

  it("should handle all known task instance routes", () => {
    const knownRoutes = [
      "logs",
      "events",
      "xcom",
      "code",
      "details",
      "rendered_templates",
      "task_instances",
      "asset_events",
      "required_actions",
    ];

    for (const route of knownRoutes) {
      const result = getTaskInstanceAdditionalPath(`/dags/test/runs/run_1/tasks/task_1/${route}`);

      expect(result).toBe(`/${route}`);
    }
  });

  it("should handle various path scenarios", () => {
    // Plugin routes
    expect(getTaskInstanceAdditionalPath("/dags/my_dag/runs/run_1/tasks/task_1/plugin/custom-view")).toBe(
      "/plugin/custom-view",
    );

    // Unknown sub-routes should return empty
    expect(getTaskInstanceAdditionalPath("/dags/my_dag/runs/run_1/tasks/task_1/unknown_route")).toBe("");

    // Complex plugin paths
    expect(
      getTaskInstanceAdditionalPath("/dags/test/runs/run_1/tasks/group/group_1/plugin/my-plugin/nested/path"),
    ).toBe("/plugin/my-plugin/nested/path");

    // Routes with special characters
    expect(
      getTaskInstanceAdditionalPath("/dags/my-dag_v2/runs/run_1-test/tasks/task.1/rendered_templates"),
    ).toBe("/rendered_templates");
  });
});

describe("getDefaultTaskInstanceTabPath", () => {
  it.each(["details", "rendered_templates", "code", "events", "asset_events", "xcom"])(
    "returns the tab path for %s",
    (tab) => {
      expect(getDefaultTaskInstanceTabPath(tab)).toBe(tab);
    },
  );

  it.each(["logs", "", "unknown_tab", undefined, 42])("falls back to the index route for %s", (value) => {
    expect(getDefaultTaskInstanceTabPath(value)).toBe("");
  });
});

describe("buildTaskInstanceUrl", () => {
  it("should build basic URL types", () => {
    // Basic task instance URL
    expect(
      buildTaskInstanceUrl({
        currentPathname: "/dags/other_dag/runs/other_run/tasks/other_task",
        dagId: "my_dag",
        runId: "run_123",
        taskId: "task_1",
      }),
    ).toBe("/dags/my_dag/runs/run_123/tasks/task_1");

    // Group task instance URL
    expect(
      buildTaskInstanceUrl({
        currentPathname: "/some/path",
        dagId: "my_dag",
        isGroup: true,
        runId: "run_123",
        taskId: "group_1",
      }),
    ).toBe("/dags/my_dag/runs/run_123/tasks/group/group_1");

    // Mapped task without map index
    expect(
      buildTaskInstanceUrl({
        currentPathname: "/some/path",
        dagId: "my_dag",
        isMapped: true,
        runId: "run_123",
        taskId: "mapped_task",
      }),
    ).toBe("/dags/my_dag/runs/run_123/tasks/mapped_task/mapped");

    // Mapped task with map index
    expect(
      buildTaskInstanceUrl({
        currentPathname: "/some/path",
        dagId: "my_dag",
        isMapped: true,
        mapIndex: "5",
        runId: "run_123",
        taskId: "mapped_task",
      }),
    ).toBe("/dags/my_dag/runs/run_123/tasks/mapped_task/mapped/5");
  });

  it("should handle advanced scenarios", () => {
    // Preserve sub-routes from current pathname
    expect(
      buildTaskInstanceUrl({
        currentPathname: "/dags/old_dag/runs/old_run/tasks/old_task/details",
        dagId: "new_dag",
        runId: "new_run",
        taskId: "new_task",
      }),
    ).toBe("/dags/new_dag/runs/new_run/tasks/new_task/details");

    // Preserve sub-routes for mapped tasks
    expect(
      buildTaskInstanceUrl({
        currentPathname: "/dags/old_dag/runs/old_run/tasks/old_task/mapped/2/xcom",
        dagId: "new_dag",
        isMapped: true,
        mapIndex: "7",
        runId: "new_run",
        taskId: "new_task",
      }),
    ).toBe("/dags/new_dag/runs/new_run/tasks/new_task/mapped/7/xcom");

    // Handle mapIndex of -1
    expect(
      buildTaskInstanceUrl({
        currentPathname: "/some/path",
        dagId: "my_dag",
        isMapped: true,
        mapIndex: "-1",
        runId: "run_123",
        taskId: "mapped_task",
      }),
    ).toBe("/dags/my_dag/runs/run_123/tasks/mapped_task/mapped");

    // Groups should never preserve tabs (only have "Task Instances" tab)
    expect(
      buildTaskInstanceUrl({
        currentPathname: "/dags/old/runs/old/tasks/old_task/rendered_templates",
        dagId: "new_dag",
        isGroup: true,
        runId: "new_run",
        taskId: "new_group",
      }),
    ).toBe("/dags/new_dag/runs/new_run/tasks/group/new_group");

    // Groups should never get /mapped appended — no such route exists for task groups
    expect(
      buildTaskInstanceUrl({
        currentPathname: "/dags/old/runs/old/tasks/group/old_group/events",
        dagId: "new_dag",
        isGroup: true,
        isMapped: true,
        mapIndex: "3",
        runId: "new_run",
        taskId: "new_group",
      }),
    ).toBe("/dags/new_dag/runs/new_run/tasks/group/new_group");
  });

  it("should not append /mapped for dynamic task groups from grid view", () => {
    // Regression test for https://github.com/apache/airflow/issues/63197
    // Dynamic task groups have isMapped=true but no route exists for group/:groupId/mapped
    expect(
      buildTaskInstanceUrl({
        currentPathname: "/dags/my_dag/runs/run_1/tasks/group/my_group",
        dagId: "my_dag",
        isGroup: true,
        isMapped: true,
        runId: "run_1",
        taskId: "my_group",
      }),
    ).toBe("/dags/my_dag/runs/run_1/tasks/group/my_group");
  });

  it("should not preserve sub-routes for mapped tasks without map index", () => {
    expect(
      buildTaskInstanceUrl({
        currentPathname: "/dags/old_dag/runs/old_run/tasks/old_task/mapped/2/xcom",
        dagId: "new_dag",
        isMapped: true,
        runId: "new_run",
        taskId: "new_task",
      }),
    ).toBe("/dags/new_dag/runs/new_run/tasks/new_task/mapped");
  });
});

describe("getNextHref", () => {
  // Regression tests for https://github.com/apache/airflow/issues/46533 — the
  // "next" parameter sent to the login redirect must be a same-origin relative
  // URL so that proxied deployments (e.g. Gitpod) don't bounce the browser
  // back to the API server's reported origin (e.g. http://localhost:29091).
  it.each([
    {
      description: "preserves pathname only",
      expected: "/dags/my_dag",
      input: { hash: "", pathname: "/dags/my_dag", search: "" },
    },
    {
      description: "preserves pathname and search",
      expected: "/dags/my_dag?tab=graph",
      input: { hash: "", pathname: "/dags/my_dag", search: "?tab=graph" },
    },
    {
      description: "preserves pathname, search, and hash",
      expected: "/dags/my_dag?tab=graph#section",
      input: { hash: "#section", pathname: "/dags/my_dag", search: "?tab=graph" },
    },
    {
      description: "preserves proxied base path, search, and hash",
      expected: "/team-a/dags/my_dag?tab=graph#section",
      input: { hash: "#section", pathname: "/team-a/dags/my_dag", search: "?tab=graph" },
    },
    {
      description: "handles root path",
      expected: "/",
      input: { hash: "", pathname: "/", search: "" },
    },
  ])("$description", ({ expected, input }) => {
    expect(getNextHref(input)).toBe(expected);
  });

  it("does not include the origin (no http(s) prefix)", () => {
    const result = getNextHref({ hash: "", pathname: "/dags/my_dag", search: "" });

    expect(result.startsWith("http://")).toBe(false);
    expect(result.startsWith("https://")).toBe(false);
  });
});

describe("getSafeExternalUrl", () => {
  describe("allows", () => {
    const safeCases = [
      ["http URL", "http://example.com/path"],
      ["https URL", "https://example.com/path?q=1#anchor"],
      ["mailto URL", "mailto:ops@example.com"],
      ["relative absolute path", "/dags/my_dag"],
      ["relative same-document fragment", "#section"],
      ["relative query string", "?owner=me"],
      ["same-origin absolute URL", `${globalThis.location.origin}/dags`],
      ["URL with surrounding whitespace", "  https://example.com/x  "],
    ];

    it.each(safeCases)("passes through a %s", (_description, input) => {
      expect(getSafeExternalUrl(input)).toBe(input.trim());
    });
  });

  describe("rejects", () => {
    const unsafeCases = [
      ["javascript: URL", "javascript:alert(1)"],
      ["javascript: URL with mixed case", "JavaScript:alert(1)"],
      ["javascript: URL with leading whitespace", " javascript:alert(1)"],
      ["data: URL", "data:text/html,<script>alert(1)</script>"],
      ["file: URL", "file:///etc/passwd"],
      ["vbscript: URL", "vbscript:msgbox(1)"],
      ["ftp: URL", "ftp://example.com/file"],
      ["empty string", ""],
      ["whitespace-only string", "   "],
    ];

    it.each(unsafeCases)("returns undefined for a %s", (_description, input) => {
      expect(getSafeExternalUrl(input)).toBeUndefined();
    });
  });
});

describe("getLinkTarget", () => {
  it.each([
    ["a relative path", "/dags/etl", "_self"],
    ["an absolute same-origin URL", `${globalThis.location.origin}/dags/etl`, "_self"],
    ["a cross-origin URL", "https://example.com/runbook", "_blank"],
    ["an unparsable URL", "http://[", "_blank"],
  ])("returns %s -> %s", (_label, url, expected) => {
    expect(getLinkTarget(url)).toBe(expected);
  });
});

describe("getInternalRouteTo", () => {
  it.each([
    ["the Dags list", "/dags"],
    ["a Dag detail page", "/dags/etl_sales"],
    ["a Dag run", "/dags/etl_sales/runs/manual__1"],
    ["a task instance", "/dags/etl_sales/runs/manual__1/tasks/load"],
    ["a plugin tab", "/dags/etl_sales/plugin/my-view"],
  ])("resolves %s to a router path", (_label, path) => {
    expect(getInternalRouteTo(path)).toBe(path);
  });

  it("keeps the query string and hash", () => {
    expect(getInternalRouteTo("/dags/etl_sales?tags=ml#notes")).toBe("/dags/etl_sales?tags=ml#notes");
  });

  it("accepts an absolute URL pointing back at this origin", () => {
    expect(getInternalRouteTo(`${globalThis.location.origin}/dags/etl_sales`)).toBe("/dags/etl_sales");
  });

  // Same origin is not enough: the API and a plugin's own FastAPI pages live here too, and have
  // to be followed as document requests rather than pushed through the router.
  it.each([
    ["the REST API", "/api/v2/plugins"],
    ["a plugin's own backend page", "/plugin-api/catalog/report"],
  ])("rejects %s, which no route owns", (_label, path) => {
    expect(getInternalRouteTo(path)).toBeUndefined();
  });

  it("rejects a cross-origin URL", () => {
    expect(getInternalRouteTo("https://example.com/dags/etl_sales")).toBeUndefined();
  });

  it("rejects an unparsable URL", () => {
    expect(getInternalRouteTo("http://[")).toBeUndefined();
  });

  describe("with the current location", () => {
    const onDagsList = { pathname: "/dags", search: "?tags=ml&paused=false" };

    it("keeps the query string the user already has when the path does not change", () => {
      expect(getInternalRouteTo("/dags?details=etl_sales", onDagsList)).toBe(
        "/dags?tags=ml&paused=false&details=etl_sales",
      );
    });

    it("keeps it even when the link carries no query of its own", () => {
      expect(getInternalRouteTo("/dags", onDagsList)).toBe("/dags?tags=ml&paused=false");
    });

    it("lets the link's value win for a key the page already had", () => {
      expect(getInternalRouteTo("/dags?tags=sales", onDagsList)).toBe("/dags?paused=false&tags=sales");
    });

    // Replacing rather than adding: otherwise a link meaning "show failed" would come out as
    // "failed as well as whatever was already selected".
    it("replaces every value a repeated key had", () => {
      expect(getInternalRouteTo("/dags?tags=sales", { pathname: "/dags", search: "?tags=ml&tags=ops" })).toBe(
        "/dags?tags=sales",
      );
    });

    it("keeps all of the link's own values when it repeats a key", () => {
      expect(getInternalRouteTo("/dags?tags=sales&tags=ops", onDagsList)).toBe(
        "/dags?paused=false&tags=sales&tags=ops",
      );
    });

    // Across pages the parameters belong to the page being left.
    it("drops the current query when the path changes", () => {
      expect(getInternalRouteTo("/dags/etl_sales?details=1", onDagsList)).toBe("/dags/etl_sales?details=1");
    });

    it("navigates clean to a different path with no query of its own", () => {
      expect(getInternalRouteTo("/dags/etl_sales", onDagsList)).toBe("/dags/etl_sales");
    });

    it("leaves the path bare when nothing survives the merge", () => {
      expect(getInternalRouteTo("/dags", { pathname: "/dags", search: "" })).toBe("/dags");
    });

    // The link's identifiers arrive percent-encoded while the router's pathname keeps them raw,
    // so comparing the two as strings would call every run-scoped page a different page.
    it("keeps the query string on a run page whose id encodes", () => {
      const runId = "manual__2026-10-20T12:00:00+00:00";
      const onRunPage = {
        pathname: `/dags/etl_sales/runs/${runId}`,
        search: "?state=failed",
      };

      expect(
        getInternalRouteTo(`/dags/etl_sales/runs/${encodeURIComponent(runId)}?details=1`, onRunPage),
      ).toBe(`/dags/etl_sales/runs/${encodeURIComponent(runId)}?state=failed&details=1`);
    });

    it("still treats a different run as a different page", () => {
      const onRunPage = {
        pathname: "/dags/etl_sales/runs/manual__2026-10-20T12:00:00+00:00",
        search: "?state=failed",
      };

      expect(
        getInternalRouteTo(
          `/dags/etl_sales/runs/${encodeURIComponent("manual__2026-10-21T12:00:00+00:00")}?details=1`,
          onRunPage,
        ),
      ).toBe(`/dags/etl_sales/runs/${encodeURIComponent("manual__2026-10-21T12:00:00+00:00")}?details=1`);
    });

    // An encoded slash is a value inside one segment, not a path boundary; splitting on the
    // decoded string would make this look like a deeper path and never match.
    it("treats an encoded slash as part of the segment, not a boundary", () => {
      const runId = "manual__2026-10-20T12:00:00+00:00/retry";
      // The router keeps the id encoded in `pathname`, so both sides carry `%2F`.
      const onRunPage = {
        pathname: `/dags/etl_sales/runs/${encodeURIComponent(runId)}`,
        search: "?state=failed",
      };

      expect(
        getInternalRouteTo(`/dags/etl_sales/runs/${encodeURIComponent(runId)}?details=1`, onRunPage),
      ).toBe(`/dags/etl_sales/runs/${encodeURIComponent(runId)}?state=failed&details=1`);
    });

    it("keeps the current page for a query-only href", () => {
      expect(getInternalRouteTo("?details=1", { pathname: "/dags", search: "?tags=ml" })).toBe(
        "/dags?tags=ml&details=1",
      );
    });

    it("keeps the current page for a hash-only href", () => {
      expect(getInternalRouteTo("#notes", { pathname: "/dags", search: "?tags=ml" })).toBe(
        "/dags?tags=ml#notes",
      );
    });

    it("keeps the link's hash alongside the merged query", () => {
      expect(getInternalRouteTo("/dags?details=etl_sales#notes", onDagsList)).toBe(
        "/dags?tags=ml&paused=false&details=etl_sales#notes",
      );
    });
  });
});

// Airflow served under `[api] base_url = http://host/airflow`. `basePath` is read from
// `<base href>` when the module loads, so each case re-imports with the tag in place.
const loadUnder = async (baseHref: string) => {
  document.head.innerHTML = `<base href="${baseHref}">`;
  vi.resetModules();

  return import("./links");
};

describe("getInternalRouteTo under a base path", () => {
  afterEach(() => {
    document.head.innerHTML = "";
    vi.resetModules();
  });

  // Guards the fixture itself: if `<base href>` were not picked up, basePath would be "" and
  // every case below would pass without exercising a prefixed deployment at all.
  it("reads the base path from the base tag", async () => {
    await loadUnder("/airflow/");
    const { basePath } = await import("src/basePath");

    expect(basePath).toBe("/airflow");
  });

  // The form the docs now recommend: the browser resolves it against `<base>`, and so do we.
  it("resolves a relative href under the base path", async () => {
    const { getInternalRouteTo: resolve } = await loadUnder("/airflow/");

    expect(resolve("dags/etl_sales/runs")).toBe("/dags/etl_sales/runs");
  });

  // Written without knowing the deployment's prefix. Previously this matched no route and
  // opened `host/dags/etl_sales/runs` in a new tab, which renders a blank page.
  it("resolves a root-absolute href under the base path", async () => {
    const { getInternalRouteTo: resolve } = await loadUnder("/airflow/");

    expect(resolve("/dags/etl_sales/runs")).toBe("/dags/etl_sales/runs");
  });

  it("accepts an href that already carries the base path", async () => {
    const { getInternalRouteTo: resolve } = await loadUnder("/airflow/");

    expect(resolve("/airflow/dags/etl_sales/runs")).toBe("/dags/etl_sales/runs");
  });

  it("still refuses a same-origin path no route owns", async () => {
    const { getInternalRouteTo: resolve } = await loadUnder("/airflow/");

    expect(resolve("/airflow/api/v2/plugins")).toBeUndefined();
  });

  it("keeps the query string of a same-page link", async () => {
    const { getInternalRouteTo: resolve } = await loadUnder("/airflow/");

    expect(resolve("/dags?details=etl_sales", { pathname: "/dags", search: "?tags=ml" })).toBe(
      "/dags?tags=ml&details=etl_sales",
    );
  });
});
