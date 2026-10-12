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
import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import { TaskInstanceService } from "openapi/requests";
import type { TaskInstanceResponse } from "openapi/requests/types.gen";

import { Wrapper } from "src/utils/Wrapper";

import { useTaskInstanceNote } from "./useTaskInstanceNote";

afterEach(() => vi.restoreAllMocks());

it("saves a note against the selected region without addressing another loop pass", async () => {
  const ti = {
    dag_id: "dag",
    dag_run_id: "run",
    id: "execution",
    map_index: -1,
    note: null,
    region_id: "11111111-1111-4111-8111-111111111111",
    region_index: 3,
    task_id: "body.work",
  } as TaskInstanceResponse;
  const patch = vi
    .spyOn(TaskInstanceService, "patchTaskInstance")
    .mockResolvedValue({ task_instances: [ti], total_entries: 1 });
  const { result } = renderHook(() => useTaskInstanceNote(ti), { wrapper: Wrapper });

  act(() => result.current.setNote("Retain this explanation"));
  act(() => result.current.onSave());
  await waitFor(() => expect(patch).toHaveBeenCalled());
  expect(patch.mock.lastCall?.[0]).toMatchObject({
    mapIndex: -1,
    requestBody: { note: "Retain this explanation", region_id: ti.region_id, region_index: 3 },
  });
});
