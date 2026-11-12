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
import type { LoaderFunctionArgs } from "react-router-dom";

import { ensureUseTaskInstanceServiceGetMappedTaskInstanceData } from "openapi/queries/ensureQueryData";
import {
  prefetchUseDagRunServiceGetDagRun,
  prefetchUseDagServiceGetDag,
  prefetchUseDagServiceGetDagDetails,
  prefetchUseDagServiceGetLatestRunInfo,
  prefetchUseTaskInstanceServiceGetLog,
} from "openapi/queries/prefetch";

import { DEFAULT_TASK_INSTANCE_TAB_KEY } from "src/constants/localStorage";
import { DEFAULT_TASK_INSTANCE_TAB_PATHS, type DefaultTaskInstanceTab } from "src/constants/tab";

// Route loaders run in parallel with the root loader (the UI config), so these requests start while the app
// loads instead of after the page's first render. Each one uses the generated prefetch helpers, so its query
// key is the one the page's own query builds and the page renders from the cache. The loaders do not wait for
// their requests and never fail, so they do not delay or break rendering; a page whose query ends up with a
// different key only sends its own request, as it does without the loaders.

const hasSearch = (request: Request) => new URL(request.url).search !== "";

export const createDagLoader =
  (queryClient: QueryClient) =>
  ({ params }: LoaderFunctionArgs) => {
    const { dagId = "" } = params;

    void prefetchUseDagServiceGetDag(queryClient, { dagId });
    void prefetchUseDagServiceGetDagDetails(queryClient, { dagId });
    void prefetchUseDagServiceGetLatestRunInfo(queryClient, { dagId });

    return null;
  };

export const createRunLoader =
  (queryClient: QueryClient) =>
  ({ params }: LoaderFunctionArgs) => {
    const { dagId = "", runId = "" } = params;

    void prefetchUseDagServiceGetDag(queryClient, { dagId });
    void prefetchUseDagRunServiceGetDagRun(queryClient, { dagId, dagRunId: runId });

    return null;
  };

const defaultTabIsLogs = () => {
  let stored: unknown = "logs";

  try {
    const raw = globalThis.localStorage.getItem(DEFAULT_TASK_INSTANCE_TAB_KEY);

    stored = raw === null ? "logs" : (JSON.parse(raw) as unknown);
  } catch {
    // Unreadable storage: the page falls back to the Logs tab, and so does this check.
  }

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

    if (hasSearch(request) || Number.isNaN(parsedMapIndex) || (isDefaultTab && !defaultTabIsLogs())) {
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
