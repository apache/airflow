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
import { renderHook, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import { TaskInstanceService } from "openapi/requests";
import type { TaskInstanceResponse } from "openapi/requests/types.gen";

import { useClearTaskInstancesDryRuns } from "./useClearTaskInstancesDryRun";

afterEach(() => vi.restoreAllMocks());

const createWrapper = (client: QueryClient) => {
  const Wrapper = ({ children }: PropsWithChildren) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  );

  return Wrapper;
};

const taskInstance = (id: string, dagRunId: string) => ({ dag_run_id: dagRunId, id }) as TaskInstanceResponse;

it("merges the previews of every request and lists a shared task instance once", async () => {
  const preview = vi.spyOn(TaskInstanceService, "postClearTaskInstances").mockImplementation(
    ({ requestBody }) =>
      Promise.resolve({
        task_instances:
          requestBody.dag_run_id === "run_1"
            ? [taskInstance("shared", "run_1"), taskInstance("only_1", "run_1")]
            : [taskInstance("shared", "run_1"), taskInstance("only_2", "run_2")],
        total_entries: 2,
      }) as ReturnType<typeof TaskInstanceService.postClearTaskInstances>,
  );
  const { result } = renderHook(
    () =>
      useClearTaskInstancesDryRuns({
        requests: [
          { dagId: "dag", requestBody: { dag_run_id: "run_1" } },
          { dagId: "dag", requestBody: { dag_run_id: "run_2" } },
        ],
      }),
    { wrapper: createWrapper(new QueryClient()) },
  );

  await waitFor(() => expect(result.current.data.total_entries).toBe(3));

  expect(result.current.data.task_instances.map(({ id }) => id)).toEqual(["shared", "only_1", "only_2"]);
  expect(
    preview.mock.calls.map(([{ requestBody }]) => [requestBody.dag_run_id, requestBody.dry_run]),
  ).toEqual([
    ["run_1", true],
    ["run_2", true],
  ]);
});

it("reports the first failed preview", async () => {
  const failure = new Error("not current");

  vi.spyOn(TaskInstanceService, "postClearTaskInstances").mockRejectedValue(failure);
  const { result } = renderHook(
    () =>
      useClearTaskInstancesDryRuns({
        requests: [{ dagId: "dag", requestBody: { dag_run_id: "run_1" } }],
      }),
    { wrapper: createWrapper(new QueryClient({ defaultOptions: { queries: { retry: false } } })) },
  );

  await waitFor(() => expect(result.current.error).toBe(failure));
});
