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
import type { QueryClient } from "@tanstack/react-query";
import dayjs from "dayjs";
import type { LoaderFunctionArgs } from "react-router-dom";

import { ensureUseTaskInstanceServiceGetMappedTaskInstanceData } from "openapi/queries/ensureQueryData";
import {
  prefetchUseAssetServiceGetAssetEvents,
  prefetchUseDagRunServiceGetDagRun,
  prefetchUseDagServiceGetDag,
  prefetchUseDagServiceGetDagDetails,
  prefetchUseDagServiceGetDagsUi,
  prefetchUseDagServiceGetLatestRunInfo,
  prefetchUseDashboardServiceDagStats,
  prefetchUseDashboardServiceHistoricalMetrics,
  prefetchUseGridServiceGetGridRuns,
  prefetchUseImportErrorServiceGetImportErrors,
  prefetchUseMonitorServiceGetHealth,
  prefetchUsePluginServiceGetPlugins,
  prefetchUsePluginServiceImportErrors,
  prefetchUsePoolServiceGetPools,
  prefetchUseTaskInstanceServiceGetLog,
} from "openapi/queries/prefetch";

import type { DagView } from "src/constants/dagView";
import {
  dagRunsLimitKey,
  DEFAULT_DAG_VIEW_KEY,
  DEFAULT_LANDING_PAGE_KEY,
  DEFAULT_TASK_INSTANCE_TAB_KEY,
} from "src/constants/localStorage";
import { SearchParamsKeys } from "src/constants/searchParams";
import { DEFAULT_TASK_INSTANCE_TAB_PATHS, type DefaultTaskInstanceTab } from "src/constants/tab";
import { DEFAULT_LIMIT as DEFAULT_DAG_RUNS_LIMIT, readLimitParam } from "src/hooks/useDagRunsLimit";

// Route loaders run in parallel with the root loader (the UI config), so these requests start while the app
// loads instead of after the page's first render. Each one uses the generated prefetch helpers, so its query
// key is the one the page's own query builds and the page renders from the cache. The loaders do not wait for
// their requests and never fail, so they do not delay or break rendering; a page whose query ends up with a
// different key only sends its own request, as it does without the loaders.

// The only URL parameters that change the task instance or log query keys. Others, such as the Logs tab's
// log level and source filters, leave the keys unchanged.
const ATTEMPT_SEARCH_PARAMS = [
  SearchParamsKeys.TRY_NUMBER,
  SearchParamsKeys.REGION_ID,
  SearchParamsKeys.REGION_INDEX,
] as const;

const hasAttemptSearchParams = (request: Request) => {
  const { searchParams } = new URL(request.url);

  return ATTEMPT_SEARCH_PARAMS.some((key) => searchParams.has(key));
};

// Reads a value the way the pages' `useLocalStorage` hooks do (JSON), falling back to their default.
const readStoredValue = (key: string, fallback: unknown): unknown => {
  try {
    const raw = globalThis.localStorage.getItem(key);

    return raw === null ? fallback : (JSON.parse(raw) as unknown);
  } catch {
    // Unreadable storage: the pages fall back to their default, and so do the loaders.
    return fallback;
  }
};

// The URL parameters that filter the Grid's runs, and so change its query key.
const GRID_FILTER_SEARCH_PARAMS = [
  SearchParamsKeys.RUN_AFTER_GTE,
  SearchParamsKeys.RUN_AFTER_LTE,
  SearchParamsKeys.RUN_ID_PATTERN,
  SearchParamsKeys.RUN_TYPE,
  SearchParamsKeys.STATE,
  SearchParamsKeys.TRIGGERING_USER_NAME_PATTERN,
] as const;

// The Grid's first page of runs, as the Grid and DetailsLayout request it: `limit` comes from the URL, else
// the Dag's stored limit, else the default, plus one to detect a next page. Skipped when the URL filters the
// runs, and in the Gantt view, whose limit also depends on the panel width. In the graph view, only
// DetailsLayout requests this page, and only on run and task pages.
const prefetchGridFirstPage = (
  queryClient: QueryClient,
  { dagId, onRunPage, request }: { dagId: string; onRunPage: boolean; request: Request },
) => {
  const { searchParams } = new URL(request.url);
  const dagView = readStoredValue(DEFAULT_DAG_VIEW_KEY, "grid") as DagView;

  if (
    GRID_FILTER_SEARCH_PARAMS.some((key) => searchParams.has(key)) ||
    !(dagView === "grid" || (dagView === "graph" && onRunPage))
  ) {
    return;
  }
  const limit =
    readLimitParam(searchParams) ?? Number(readStoredValue(dagRunsLimitKey(dagId), DEFAULT_DAG_RUNS_LIMIT));

  void prefetchUseGridServiceGetGridRuns(queryClient, {
    dagId,
    limit: limit + 1,
    offset: 0,
    orderBy: ["-run_after"],
  });
};

export const createDagLoader =
  (queryClient: QueryClient) =>
  ({ params, request }: LoaderFunctionArgs) => {
    const { dagId = "" } = params;

    void prefetchUseDagServiceGetDag(queryClient, { dagId });
    void prefetchUseDagServiceGetDagDetails(queryClient, { dagId });
    void prefetchUseDagServiceGetLatestRunInfo(queryClient, { dagId });
    prefetchGridFirstPage(queryClient, { dagId, onRunPage: false, request });

    return null;
  };

export const createRunLoader =
  (queryClient: QueryClient) =>
  ({ params, request }: LoaderFunctionArgs) => {
    const { dagId = "", runId = "" } = params;

    void prefetchUseDagServiceGetDag(queryClient, { dagId });
    // The breadcrumb's query, which every run and task page renders.
    void prefetchUseDagServiceGetDagDetails(queryClient, { dagId });
    void prefetchUseDagRunServiceGetDagRun(queryClient, { dagId, dagRunId: runId });
    prefetchGridFirstPage(queryClient, { dagId, onRunPage: true, request });

    return null;
  };

const defaultTabIsLogs = () => {
  const stored = readStoredValue(DEFAULT_TASK_INSTANCE_TAB_KEY, "logs");

  return (
    typeof stored !== "string" ||
    !(stored in DEFAULT_TASK_INSTANCE_TAB_PATHS) ||
    DEFAULT_TASK_INSTANCE_TAB_PATHS[stored as DefaultTaskInstanceTab] === ""
  );
};

// The log request needs the task instance's try number: fetch the task instance, then its log. Only for the
// current try at the default coordinates (no try_number, region_id or region_index in the URL), which is
// what the Logs tab requests when nothing in the URL pins another attempt or loop iteration.
export const createTaskLogsLoader =
  (queryClient: QueryClient, { isDefaultTab }: { isDefaultTab: boolean }) =>
  ({ params, request }: LoaderFunctionArgs) => {
    const { dagId = "", mapIndex = "-1", runId = "", taskId = "" } = params;
    const parsedMapIndex = Number(mapIndex);

    if (
      hasAttemptSearchParams(request) ||
      Number.isNaN(parsedMapIndex) ||
      (isDefaultTab && !defaultTabIsLogs())
    ) {
      return null;
    }

    void ensureUseTaskInstanceServiceGetMappedTaskInstanceData(queryClient, {
      dagId,
      dagRunId: runId,
      mapIndex: parsedMapIndex,
      taskId,
    })
      .then((taskInstance) =>
        // Like the Logs tab: no log request for a try that has not started yet.
        taskInstance.try_number < 1 || taskInstance.state === null || taskInstance.state === "up_for_retry"
          ? undefined
          : prefetchUseTaskInstanceServiceGetLog(queryClient, {
              accept: "application/x-ndjson",
              dagId,
              dagRunId: taskInstance.dag_run_id,
              mapIndex: taskInstance.map_index,
              // Passed as the Logs tab passes them, so the query keys match.
              regionId: taskInstance.region_id,
              regionIndex: taskInstance.region_index,
              taskId: taskInstance.task_id,
              tryNumber: taskInstance.try_number,
            }),
      )
      .catch(() => undefined);

    return null;
  };

// The Dashboard shows the last DASHBOARD_DEFAULT_HOURS hours, counted from when its range is created. Its
// history and asset event queries carry that range, so the loader creates it and the Dashboard starts from the
// same range, which makes the keys match. A range is reused for DASHBOARD_RANGE_REUSE_MS at most.
export const DASHBOARD_DEFAULT_HOURS = 24;
const DASHBOARD_RANGE_REUSE_MS = 10_000;

let lastDashboardRange: { createdAt: number; endDate: string; startDate: string } | undefined;

/** The Dashboard's initial range: the one created in the last few seconds (by its loader), else a new one. */
export const getDashboardRange = () => {
  if (
    lastDashboardRange === undefined ||
    Date.now() - lastDashboardRange.createdAt >= DASHBOARD_RANGE_REUSE_MS
  ) {
    const now = dayjs();

    lastDashboardRange = {
      createdAt: now.valueOf(),
      endDate: now.toISOString(),
      startDate: now.subtract(DASHBOARD_DEFAULT_HOURS, "hour").toISOString(),
    };
  }

  return lastDashboardRange;
};

// The Dashboard's queries. On the app root, only when the user's landing page is the Dashboard, which is what
// the root renders unless the user chose the Dags list.
export const createDashboardLoader =
  (queryClient: QueryClient, { isLandingPage }: { isLandingPage: boolean }) =>
  () => {
    if (isLandingPage && readStoredValue(DEFAULT_LANDING_PAGE_KEY, "dashboard") === "dags") {
      return null;
    }
    const { endDate, startDate } = getDashboardRange();

    void prefetchUseDashboardServiceHistoricalMetrics(queryClient, { startDate });
    void prefetchUseAssetServiceGetAssetEvents(queryClient, {
      limit: 6,
      orderBy: ["-timestamp"],
      timestampGte: startDate,
      timestampLte: endDate,
    });
    void prefetchUseDashboardServiceDagStats(queryClient);
    void prefetchUseMonitorServiceGetHealth(queryClient);
    void prefetchUseDagServiceGetDagsUi(queryClient, { isFavorite: true, limit: 10 });
    void prefetchUseImportErrorServiceGetImportErrors(queryClient, { limit: 1 });
    void prefetchUsePluginServiceImportErrors(queryClient);
    void prefetchUsePoolServiceGetPools(queryClient);
    void prefetchUsePluginServiceGetPlugins(queryClient);

    return null;
  };
