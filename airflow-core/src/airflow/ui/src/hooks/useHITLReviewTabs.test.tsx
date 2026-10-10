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
import axios from "axios";
import { afterEach, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { useHITLReviewTabs } from "./useHITLReviewTabs";

afterEach(() => vi.restoreAllMocks());

it("discovers provider review sessions independently for each selected region", async () => {
  const fetch = vi.spyOn(axios, "get").mockResolvedValue({ data: {} });
  const { rerender } = renderHook(
    ({ regionId }) =>
      useHITLReviewTabs(
        { dagId: "dag", dagRunId: "run", taskId: "work" },
        [{ icon: null, label: "Review", value: "plugin/hitl-review" }],
        { mapIndex: -1, regionId, regionIndex: 3 },
      ),
    {
      initialProps: { regionId: "11111111-1111-4111-8111-111111111111" },
      wrapper: Wrapper,
    },
  );

  await waitFor(() => expect(fetch).toHaveBeenCalledTimes(1));
  expect(fetch.mock.lastCall?.[1]?.params).toEqual({
    dag_id: "dag",
    map_index: -1,
    region_id: "11111111-1111-4111-8111-111111111111",
    region_index: 3,
    run_id: "run",
    task_id: "work",
  });
  rerender({ regionId: "22222222-2222-4222-8222-222222222222" });
  await waitFor(() => expect(fetch).toHaveBeenCalledTimes(2));
  expect(fetch.mock.lastCall?.[1]?.params).toMatchObject({
    region_id: "22222222-2222-4222-8222-222222222222",
  });
});
