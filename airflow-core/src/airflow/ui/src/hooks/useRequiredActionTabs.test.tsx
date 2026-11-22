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

import { Wrapper } from "src/utils/Wrapper";

import { useRequiredActionTabs } from "./useRequiredActionTabs";

const REGION_ID = "11111111-1111-4111-8111-111111111111";

afterEach(() => vi.restoreAllMocks());

it("answers the exact lookup from the collection rather than the single detail", async () => {
  const collection = vi
    .spyOn(TaskInstanceService, "getHitlDetails")
    .mockResolvedValue({ hitl_details: [], total_entries: 0 });
  const singleDetail = vi.spyOn(TaskInstanceService, "getHitlDetail");

  renderHook(
    () =>
      useRequiredActionTabs(
        {
          dagId: "dag",
          dagRunId: "run",
          mapIndex: -1,
          regionId: REGION_ID,
          regionIndex: 3,
          taskId: "work",
        },
        [],
      ),
    { wrapper: Wrapper },
  );

  await waitFor(() => expect(collection).toHaveBeenCalledTimes(1));
  expect(collection.mock.lastCall?.[0]).toMatchObject({
    dagId: "dag",
    dagRunId: "run",
    mapIndex: -1,
    regionId: REGION_ID,
    regionIndex: 3,
    taskId: "work",
  });
  // The single-detail endpoint says "not human-in-the-loop" with a 404, which every ordinary task
  // then logged as a failed request. Asking it at all is the bug.
  expect(singleDetail).not.toHaveBeenCalled();
});
