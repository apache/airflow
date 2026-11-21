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
  UseDagRunServiceGetDagRunKeyFn,
  UseDagServiceGetDagDetailsKeyFn,
  UseDagServiceGetDagKeyFn,
  UseDagServiceGetLatestRunInfoKeyFn,
  UseTaskInstanceServiceGetLogKeyFn,
  UseTaskInstanceServiceGetMappedTaskInstanceKeyFn,
} from "openapi/queries";
import { DagRunService, DagService, TaskInstanceService } from "openapi/requests/services.gen";
import type { TaskInstanceResponse } from "openapi/requests/types.gen";

import { DEFAULT_TASK_INSTANCE_TAB_KEY } from "src/constants/localStorage";

import { createDagLoader, createRunLoader, createTaskLogsLoader } from "./routePrefetch";

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
});
