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
import type { PropsWithChildren } from "react";

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import { useDagRunServiceGetExecutionKey } from "openapi/queries";
import { TaskInstanceService } from "openapi/requests";

import { useClearTaskInstances } from "./useClearTaskInstances";

afterEach(() => vi.restoreAllMocks());

it("invalidates the run named in the request rather than the run the hook was opened on", async () => {
  vi.spyOn(TaskInstanceService, "postClearTaskInstances").mockResolvedValue({
    task_instances: [],
    total_entries: 0,
  });
  const queryClient = new QueryClient();
  const openedRun = [useDagRunServiceGetExecutionKey, { dagId: "dag", dagRunId: "opened" }];
  const clearedRun = [useDagRunServiceGetExecutionKey, { dagId: "dag", dagRunId: "cleared" }];

  queryClient.setQueryData(openedRun, {});
  queryClient.setQueryData(clearedRun, {});
  const wrapper = ({ children }: PropsWithChildren) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );
  const onSuccessConfirm = vi.fn();
  const { result } = renderHook(() => useClearTaskInstances({ dagRunId: "opened", onSuccessConfirm }), {
    wrapper,
  });

  act(() => result.current.mutate({ dagId: "dag", requestBody: { dag_run_id: "cleared" } }));
  await waitFor(() => expect(onSuccessConfirm).toHaveBeenCalled());

  expect(queryClient.getQueryState(clearedRun)?.isInvalidated).toBe(true);
  expect(queryClient.getQueryState(openedRun)?.isInvalidated).toBe(false);
});

it("invalidates the Dag named in each request when one clear spans several Dags", async () => {
  vi.spyOn(TaskInstanceService, "postClearTaskInstances").mockResolvedValue({
    task_instances: [],
    total_entries: 0,
  });
  const queryClient = new QueryClient();
  const firstDagRun = [useDagRunServiceGetExecutionKey, { dagId: "first", dagRunId: "run" }];
  const secondDagRun = [useDagRunServiceGetExecutionKey, { dagId: "second", dagRunId: "run" }];

  queryClient.setQueryData(firstDagRun, {});
  queryClient.setQueryData(secondDagRun, {});
  const wrapper = ({ children }: PropsWithChildren) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );
  const onSuccessConfirm = vi.fn();
  const { result } = renderHook(() => useClearTaskInstances({ dagRunId: "run", onSuccessConfirm }), {
    wrapper,
  });

  act(() => result.current.mutate({ dagId: "second", requestBody: { dag_run_id: "run" } }));
  await waitFor(() => expect(onSuccessConfirm).toHaveBeenCalled());

  expect(queryClient.getQueryState(secondDagRun)?.isInvalidated).toBe(true);
  expect(queryClient.getQueryState(firstDagRun)?.isInvalidated).toBe(false);
});
