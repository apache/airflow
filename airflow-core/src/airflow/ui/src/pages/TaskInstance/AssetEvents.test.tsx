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
import { render, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, expect, it, vi } from "vitest";

import { AssetService, TaskInstanceService } from "openapi/requests";
import type { TaskInstanceResponse } from "openapi/requests/types.gen";

import { BaseWrapper } from "src/utils/Wrapper";

import { AssetEvents } from "./AssetEvents";

vi.mock("src/queries/useConfig", () => ({ useConfig: () => false }));
afterEach(() => vi.restoreAllMocks());

it("uses the selected producer UUID for regional asset events", async () => {
  const regionId = "11111111-1111-4111-8111-111111111111";
  const task = vi
    .spyOn(TaskInstanceService, "getMappedTaskInstance")
    .mockResolvedValue({ id: "selected-uuid", region_id: regionId, region_index: 3 } as TaskInstanceResponse);
  const events = vi
    .spyOn(AssetService, "getAssetEvents")
    .mockResolvedValue({ asset_events: [], total_entries: 0 });

  render(
    <BaseWrapper>
      <MemoryRouter
        initialEntries={[
          `/dags/dag/runs/run/tasks/work/asset_events?region_id=${regionId}&region_index=3&map_index=-1&try_number=2`,
        ]}
      >
        <Routes>
          <Route element={<AssetEvents />} path="/dags/:dagId/runs/:runId/tasks/:taskId/asset_events" />
        </Routes>
      </MemoryRouter>
    </BaseWrapper>,
  );
  await waitFor(() => expect(events).toHaveBeenCalled());
  expect(task.mock.lastCall?.[0]).toMatchObject({ regionId, regionIndex: 3 });
  expect(events.mock.lastCall?.[0]).toMatchObject({
    sourceMapIndex: undefined,
    sourceTaskInstanceId: "selected-uuid",
  });
});
