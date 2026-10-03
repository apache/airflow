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
import "@testing-library/jest-dom/vitest";
import { render, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import { XcomService } from "openapi/requests";

import { Wrapper } from "src/utils/Wrapper";

import { XComEntry } from "./XComEntry";

afterEach(() => vi.restoreAllMocks());

it("fetches a retained value by its own region even when the public map index collides", async () => {
  const coordinates = {
    dag_id: "dag",
    map_index: -1,
    region_id: "11111111-1111-1111-1111-111111111111",
    region_index: 3,
    run_id: "run",
    task_id: "member",
  };
  const fetch = vi.spyOn(XcomService, "getXcomEntry").mockResolvedValue({
    ...coordinates,
    dag_display_name: "dag",
    key: "return_value",
    logical_date: null,
    run_after: "2026-01-01T00:00:00Z",
    task_display_name: "member",
    timestamp: "2026-01-01T00:00:00Z",
    value: "third pass",
  });

  render(
    <XComEntry
      dagId="dag"
      mapIndex={-1}
      regionId={coordinates.region_id}
      regionIndex={3}
      runId="run"
      taskId="member"
      xcomKey="return_value"
    />,
    { wrapper: Wrapper },
  );

  expect(await screen.findByText("third pass")).toBeVisible();
  await waitFor(() =>
    expect(fetch).toHaveBeenCalledWith(
      expect.objectContaining({ mapIndex: -1, regionId: coordinates.region_id, regionIndex: 3 }),
    ),
  );
});
