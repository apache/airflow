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
import { renderHook, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import { TaskInstanceService } from "openapi/requests";
import { CancelablePromise } from "openapi/requests/core/CancelablePromise";
import type { TaskInstanceResponse } from "openapi/requests/types.gen";

import { Wrapper } from "src/utils/Wrapper";

import { useBulkMarkAsDryRun } from "./useBulkMarkAsDryRun";

afterEach(() => vi.restoreAllMocks());

it("keeps two regional executions with identical public coordinates in the bulk preview", async () => {
  const tasks = ["11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222"].map(
    (regionId, index) =>
      ({
        dag_id: "dag",
        dag_run_id: "run",
        id: `execution-${index}`,
        map_index: -1,
        region_id: regionId,
        region_index: 3,
        state: "failed",
        task_id: "body.work",
      }) as TaskInstanceResponse,
  );
  const preview = vi.spyOn(TaskInstanceService, "patchTaskInstanceDryRun").mockImplementation(
    ({ requestBody }) =>
      new CancelablePromise((resolve) =>
        resolve({
          task_instances: tasks.filter((ti) => ti.region_id === requestBody.region_id),
          total_entries: 1,
        }),
      ),
  );
  const { result } = renderHook(
    () =>
      useBulkMarkAsDryRun(true, {
        options: {
          includeDownstream: false,
          includeFuture: false,
          includePast: false,
          includeUpstream: false,
        },
        selectedTaskInstances: tasks,
        targetState: "success",
      }),
    { wrapper: Wrapper },
  );

  await waitFor(() => expect(result.current.data.total_entries).toBe(2));
  expect(preview).toHaveBeenCalledTimes(2);
  expect(result.current.data.task_instances.map((ti) => ti.id)).toEqual(["execution-0", "execution-1"]);
  expect(preview.mock.calls.map(([request]) => request.requestBody.region_id)).toEqual(
    tasks.map((ti) => ti.region_id),
  );
});
