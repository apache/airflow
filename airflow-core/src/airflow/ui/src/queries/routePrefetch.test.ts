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
import { QueryClient } from "@tanstack/react-query";
import type { LoaderFunctionArgs } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  UseAssetServiceGetAssetEventsKeyFn,
  UseDagRunServiceGetDagRunKeyFn,
  UseDagServiceGetDagDetailsKeyFn,
  UseDagServiceGetDagKeyFn,
  UseDagServiceGetDagsUiKeyFn,
  UseDagServiceGetLatestRunInfoKeyFn,
  UseDashboardServiceDagStatsKeyFn,
  UseDashboardServiceHistoricalMetricsKeyFn,
  UseGridServiceGetGridRunsKeyFn,
  UseTaskInstanceServiceGetLogKeyFn,
  UseTaskInstanceServiceGetMappedTaskInstanceKeyFn,
} from "openapi/queries";
import {
  AssetService,
  DagRunService,
  DagService,
  DashboardService,
  GridService,
  ImportErrorService,
  MonitorService,
  PluginService,
  PoolService,
  TaskInstanceService,
} from "openapi/requests/services.gen";
import type { TaskInstanceResponse } from "openapi/requests/types.gen";

import {
  dagRunsLimitKey,
  DEFAULT_DAG_VIEW_KEY,
  DEFAULT_LANDING_PAGE_KEY,
  DEFAULT_TASK_INSTANCE_TAB_KEY,
} from "src/constants/localStorage";

import {
  createDagLoader,
  createDashboardLoader,
  createRunLoader,
  createTaskLogsLoader,
  getDashboardRange,
} from "./routePrefetch";

const BASE = "http://localhost/dags/my_dag/runs/run_1/tasks/my_task";

const args = (params: Record<string, string>, url = BASE) =>
  ({ params, request: new Request(url) }) as unknown as LoaderFunctionArgs;

const taskInstance = (overrides: Partial<TaskInstanceResponse> = {}) =>
  ({
    dag_id: "my_dag",
    dag_run_id: "run_1",
    map_index: -1,
    state: "success",
    task_id: "my_task",
    try_number: 2,
    ...overrides,
  }) as TaskInstanceResponse;

const taskParams = { dagId: "my_dag", runId: "run_1", taskId: "my_task" };

// The Grid's first page as Grid and DetailsLayout request it with no filter in the URL.
const gridKey = (limit: number) =>
  UseGridServiceGetGridRunsKeyFn({ dagId: "my_dag", limit, offset: 0, orderBy: ["-run_after"] });

describe("route prefetch loaders", () => {
  let queryClient: QueryClient;

  beforeEach(() => {
    queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    vi.spyOn(DagService, "getDag").mockResolvedValue({ dag_id: "my_dag" } as never);
    vi.spyOn(DagService, "getDagDetails").mockResolvedValue({ dag_id: "my_dag" } as never);
    vi.spyOn(DagService, "getLatestRunInfo").mockResolvedValue(null);
    vi.spyOn(DagRunService, "getDagRun").mockResolvedValue({ dag_run_id: "run_1" } as never);
    vi.spyOn(TaskInstanceService, "getMappedTaskInstance").mockResolvedValue(taskInstance());
    vi.spyOn(TaskInstanceService, "getLog").mockResolvedValue({ content: [] } as never);
    vi.spyOn(GridService, "getGridRuns").mockResolvedValue([]);
  });

  afterEach(() => {
    vi.restoreAllMocks();
    globalThis.localStorage.clear();
  });

  it("caches the Dag page's queries under the keys its hooks use, without waiting for them", async () => {
    expect(createDagLoader(queryClient)(args({ dagId: "my_dag" }))).toBeNull();

    await vi.waitFor(() => {
      expect(queryClient.getQueryData(UseDagServiceGetDagKeyFn({ dagId: "my_dag" }))).toBeDefined();
      expect(queryClient.getQueryData(UseDagServiceGetDagDetailsKeyFn({ dagId: "my_dag" }))).toBeDefined();
      expect(queryClient.getQueryState(UseDagServiceGetLatestRunInfoKeyFn({ dagId: "my_dag" }))?.status).toBe(
        "success",
      );
    });
  });

  it("caches the Dag, its details and the run for run and task pages", async () => {
    createRunLoader(queryClient)(args({ dagId: "my_dag", runId: "run_1" }));

    await vi.waitFor(() => {
      expect(queryClient.getQueryData(UseDagServiceGetDagKeyFn({ dagId: "my_dag" }))).toBeDefined();
      expect(queryClient.getQueryData(UseDagServiceGetDagDetailsKeyFn({ dagId: "my_dag" }))).toBeDefined();
      expect(
        queryClient.getQueryData(UseDagRunServiceGetDagRunKeyFn({ dagId: "my_dag", dagRunId: "run_1" })),
      ).toBeDefined();
    });
  });

  it("fetches the task instance, then the log of its current try under the Logs tab's key", async () => {
    vi.mocked(TaskInstanceService.getMappedTaskInstance).mockResolvedValue(
      taskInstance({ region_id: "loop", region_index: 3 }),
    );

    expect(createTaskLogsLoader(queryClient, { isDefaultTab: false })(args(taskParams))).toBeNull();

    const logKey = UseTaskInstanceServiceGetLogKeyFn({
      accept: "application/x-ndjson",
      dagId: "my_dag",
      dagRunId: "run_1",
      mapIndex: -1,
      regionId: "loop",
      regionIndex: 3,
      taskId: "my_task",
      tryNumber: 2,
    });

    await vi.waitFor(() => expect(queryClient.getQueryData(logKey)).toBeDefined());
    expect(
      queryClient.getQueryData(
        UseTaskInstanceServiceGetMappedTaskInstanceKeyFn({
          dagId: "my_dag",
          dagRunId: "run_1",
          mapIndex: -1,
          taskId: "my_task",
        }),
      ),
    ).toBeDefined();
  });

  it("uses the map index from the route", async () => {
    vi.mocked(TaskInstanceService.getMappedTaskInstance).mockResolvedValue(taskInstance({ map_index: 4 }));

    createTaskLogsLoader(queryClient, { isDefaultTab: false })(args({ ...taskParams, mapIndex: "4" }));

    await vi.waitFor(() => expect(TaskInstanceService.getLog).toHaveBeenCalled());
    expect(TaskInstanceService.getMappedTaskInstance).toHaveBeenCalledWith(
      expect.objectContaining({ mapIndex: 4 }),
    );
    expect(TaskInstanceService.getLog).toHaveBeenCalledWith(expect.objectContaining({ mapIndex: 4 }));
  });

  it.each([
    ["a try_number", `${BASE}?try_number=1`],
    ["loop coordinates", `${BASE}?region_id=loop&region_index=0`],
  ])("leaves URLs with %s to the page", (_, url) => {
    createTaskLogsLoader(queryClient, { isDefaultTab: false })(args(taskParams, url));

    expect(TaskInstanceService.getMappedTaskInstance).not.toHaveBeenCalled();
  });

  it("prefetches the log when the URL only carries the Logs tab's filters", async () => {
    createTaskLogsLoader(queryClient, { isDefaultTab: false })(
      args(taskParams, `${BASE}?log_level=error&log_source=task`),
    );

    await vi.waitFor(() => expect(TaskInstanceService.getLog).toHaveBeenCalled());
  });

  it.each([
    ["has not run", { try_number: 0 }],
    ["has no state yet", { state: null }],
    ["waits for a retry", { state: "up_for_retry" as const }],
  ])("does not request a log for a try that %s", async (_, overrides) => {
    vi.mocked(TaskInstanceService.getMappedTaskInstance).mockResolvedValue(taskInstance(overrides));

    createTaskLogsLoader(queryClient, { isDefaultTab: false })(args(taskParams));

    await vi.waitFor(() => expect(TaskInstanceService.getMappedTaskInstance).toHaveBeenCalled());
    await Promise.resolve();
    expect(TaskInstanceService.getLog).not.toHaveBeenCalled();
  });

  it("skips the default tab when the user's default task instance tab is not Logs", () => {
    globalThis.localStorage.setItem(DEFAULT_TASK_INSTANCE_TAB_KEY, JSON.stringify("details"));

    createTaskLogsLoader(queryClient, { isDefaultTab: true })(args(taskParams));
    expect(TaskInstanceService.getMappedTaskInstance).not.toHaveBeenCalled();

    // The Logs tab itself is always prefetched.
    createTaskLogsLoader(queryClient, { isDefaultTab: false })(args(taskParams));
    expect(TaskInstanceService.getMappedTaskInstance).toHaveBeenCalledTimes(1);
  });

  it("prefetches the default tab when the default task instance tab is Logs or unset", () => {
    createTaskLogsLoader(queryClient, { isDefaultTab: true })(args(taskParams));
    expect(TaskInstanceService.getMappedTaskInstance).toHaveBeenCalledTimes(1);

    queryClient.clear();
    globalThis.localStorage.setItem(DEFAULT_TASK_INSTANCE_TAB_KEY, JSON.stringify("logs"));
    createTaskLogsLoader(queryClient, { isDefaultTab: true })(args(taskParams));
    expect(TaskInstanceService.getMappedTaskInstance).toHaveBeenCalledTimes(2);
  });

  it("swallows a failed task instance request and leaves the page to show the error", async () => {
    vi.mocked(TaskInstanceService.getMappedTaskInstance).mockRejectedValue(new Error("not found"));

    expect(createTaskLogsLoader(queryClient, { isDefaultTab: false })(args(taskParams))).toBeNull();

    await vi.waitFor(() =>
      expect(
        queryClient.getQueryState(
          UseTaskInstanceServiceGetMappedTaskInstanceKeyFn({
            dagId: "my_dag",
            dagRunId: "run_1",
            mapIndex: -1,
            taskId: "my_task",
          }),
        )?.status,
      ).toBe("error"),
    );
    expect(TaskInstanceService.getLog).not.toHaveBeenCalled();
  });

  it("prefetches the Grid's first page with the default limit plus one", async () => {
    createDagLoader(queryClient)(args({ dagId: "my_dag" }, "http://localhost/dags/my_dag"));

    await vi.waitFor(() => expect(queryClient.getQueryData(gridKey(11))).toBeDefined());
  });

  it("uses the URL's limit, else the Dag's stored limit, for the Grid's first page", async () => {
    globalThis.localStorage.setItem(dagRunsLimitKey("my_dag"), JSON.stringify(25));
    createRunLoader(queryClient)(
      args({ dagId: "my_dag", runId: "run_1" }, "http://localhost/dags/my_dag/runs/run_1"),
    );
    await vi.waitFor(() => expect(queryClient.getQueryData(gridKey(26))).toBeDefined());

    createRunLoader(queryClient)(
      args({ dagId: "my_dag", runId: "run_1" }, "http://localhost/dags/my_dag/runs/run_1?limit=5"),
    );
    await vi.waitFor(() => expect(queryClient.getQueryData(gridKey(6))).toBeDefined());
  });

  it.each([
    ["the URL filters the runs", "http://localhost/dags/my_dag?state=failed", "grid"],
    ["the Gantt view is shown", "http://localhost/dags/my_dag", "gantt"],
    ["the graph view shows no Grid on the Dag page", "http://localhost/dags/my_dag", "graph"],
  ])("leaves the Grid's runs to the page when %s", (_, url, dagView) => {
    globalThis.localStorage.setItem(DEFAULT_DAG_VIEW_KEY, JSON.stringify(dagView));

    createDagLoader(queryClient)(args({ dagId: "my_dag" }, url));

    expect(GridService.getGridRuns).not.toHaveBeenCalled();
  });

  it("prefetches the Grid's first page on run pages in the graph view, which DetailsLayout requests", async () => {
    globalThis.localStorage.setItem(DEFAULT_DAG_VIEW_KEY, JSON.stringify("graph"));

    createRunLoader(queryClient)(
      args({ dagId: "my_dag", runId: "run_1" }, "http://localhost/dags/my_dag/runs/run_1"),
    );

    await vi.waitFor(() => expect(queryClient.getQueryData(gridKey(11))).toBeDefined());
  });
});

describe("Dashboard route prefetch", () => {
  let queryClient: QueryClient;

  beforeEach(() => {
    queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    vi.spyOn(DashboardService, "historicalMetrics").mockResolvedValue({} as never);
    vi.spyOn(DashboardService, "dagStats").mockResolvedValue({} as never);
    vi.spyOn(AssetService, "getAssetEvents").mockResolvedValue({ asset_events: [], total_entries: 0 });
    vi.spyOn(MonitorService, "getHealth").mockResolvedValue({} as never);
    vi.spyOn(DagService, "getDagsUi").mockResolvedValue({ dags: [], total_entries: 0 });
    vi.spyOn(ImportErrorService, "getImportErrors").mockResolvedValue({
      import_errors: [],
      total_entries: 0,
    });
    vi.spyOn(PluginService, "importErrors").mockResolvedValue({ import_errors: [], total_entries: 0 });
    vi.spyOn(PluginService, "getPlugins").mockResolvedValue({ plugins: [], total_entries: 0 });
    vi.spyOn(PoolService, "getPools").mockResolvedValue({ pools: [], total_entries: 0 });
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
    globalThis.localStorage.clear();
  });

  it("caches the Dashboard's queries, with its history range shared with the Dashboard", async () => {
    expect(createDashboardLoader(queryClient, { isLandingPage: false })()).toBeNull();

    // The Dashboard mounts right after and starts from the same range, so its keys match.
    const { endDate, startDate } = getDashboardRange();

    await vi.waitFor(() => {
      expect(
        queryClient.getQueryData(UseDashboardServiceHistoricalMetricsKeyFn({ startDate })),
      ).toBeDefined();
      expect(
        queryClient.getQueryData(
          UseAssetServiceGetAssetEventsKeyFn({
            limit: 6,
            orderBy: ["-timestamp"],
            timestampGte: startDate,
            timestampLte: endDate,
          }),
        ),
      ).toBeDefined();
      expect(queryClient.getQueryData(UseDashboardServiceDagStatsKeyFn())).toBeDefined();
      expect(
        queryClient.getQueryData(UseDagServiceGetDagsUiKeyFn({ isFavorite: true, limit: 10 })),
      ).toBeDefined();
    });
    expect(PoolService.getPools).toHaveBeenCalled();
    expect(PluginService.getPlugins).toHaveBeenCalled();
  });

  it("creates a new range once the last one is more than a few seconds old", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2099-01-01T10:00:00Z"));
    const first = getDashboardRange();

    vi.setSystemTime(new Date("2099-01-01T10:00:05Z"));
    expect(getDashboardRange()).toEqual(first);

    vi.setSystemTime(new Date("2099-01-01T10:00:30Z"));
    expect(getDashboardRange().endDate).toBe("2099-01-01T10:00:30.000Z");
    expect(getDashboardRange().startDate).toBe("2098-12-31T10:00:30.000Z");
  });

  it("skips the app root when the user's landing page is the Dags list, but not /home", () => {
    globalThis.localStorage.setItem(DEFAULT_LANDING_PAGE_KEY, JSON.stringify("dags"));

    createDashboardLoader(queryClient, { isLandingPage: true })();
    expect(DashboardService.historicalMetrics).not.toHaveBeenCalled();

    createDashboardLoader(queryClient, { isLandingPage: false })();
    expect(DashboardService.historicalMetrics).toHaveBeenCalledTimes(1);
  });
});
