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

import { UseTaskInstanceServiceGetTaskInstanceKeyFn } from "openapi/queries";
import { TaskInstanceService } from "openapi/requests";

import { usePatchTaskInstance } from "./usePatchTaskInstance";

afterEach(() => vi.restoreAllMocks());

it("invalidates the cached task instance of a region-addressed execution", async () => {
  vi.spyOn(TaskInstanceService, "patchTaskInstance").mockResolvedValue({
    task_instances: [],
    total_entries: 0,
  });
  const queryClient = new QueryClient();
  const cached = UseTaskInstanceServiceGetTaskInstanceKeyFn({
    dagId: "dag",
    dagRunId: "run",
    regionId: "11111111-1111-4111-8111-111111111111",
    regionIndex: 3,
    taskId: "body.work",
  });

  queryClient.setQueryData(cached, {});
  const wrapper = ({ children }: PropsWithChildren) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );
  const onSuccess = vi.fn();
  const { result } = renderHook(
    () => usePatchTaskInstance({ dagId: "dag", dagRunId: "run", onSuccess, taskId: "body.work" }),
    { wrapper },
  );

  act(() => result.current.mutate({ dagId: "dag", dagRunId: "run", requestBody: {}, taskId: "body.work" }));
  await waitFor(() => expect(onSuccess).toHaveBeenCalled());

  expect(queryClient.getQueryState(cached)?.isInvalidated).toBe(true);
});
