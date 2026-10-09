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
import "@testing-library/jest-dom";
import { render, screen } from "@testing-library/react";
import type * as ReactI18Next from "react-i18next";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, expect, it, vi } from "vitest";

import { AssetStateStoreService } from "openapi/requests";
import type { AssetStateStoreLastUpdatedBy } from "openapi/requests";

import { TimezoneProvider } from "src/context/timezone";
import { BaseWrapper } from "src/utils/Wrapper";

import { AssetStateStore } from "./AssetStateStore";

vi.mock("src/queries/useConfig", () => ({ useConfig: () => 25 }));

vi.mock("react-i18next", async (importOriginal) => ({
  ...(await importOriginal<typeof ReactI18Next>()),
  useTranslation: () => ({
    i18n: { language: "en" },
    // eslint-disable-next-line id-length
    t: (key: string) => key,
  }),
}));

afterEach(() => vi.restoreAllMocks());

it.each([
  {
    expected: "/dags/dag/runs/run/tasks/work",
    identity: { try_number: 2 },
    label: "writer outside any region",
    mapIndex: -1,
  },
  {
    expected:
      "/dags/dag/runs/run/tasks/work?region_id=11111111-1111-4111-8111-111111111111&region_index=3&try_number=2",
    identity: {
      region_id: "11111111-1111-4111-8111-111111111111",
      region_index: 3,
      task_instance_id: "22222222-2222-4222-8222-222222222222",
      try_number: 2,
    },
    label: "retained loop writer",
    mapIndex: -1,
  },
  {
    expected:
      "/dags/dag/runs/run/tasks/work/mapped/0?region_id=11111111-1111-4111-8111-111111111111&region_index=0&try_number=1",
    identity: {
      region_id: "11111111-1111-4111-8111-111111111111",
      region_index: 0,
      try_number: 1,
    },
    label: "mapped writer at index zero",
    mapIndex: 0,
  },
  {
    expected: "/dags/dag/runs/run/tasks/work/mapped/2",
    identity: {},
    label: "legacy writer",
    mapIndex: 2,
  },
  {
    expected: "/dags/dag/runs/run/tasks/work",
    identity: { region_id: "11111111-1111-4111-8111-111111111111", region_index: 3 },
    label: "incomplete retained identity",
    mapIndex: -1,
  },
])("links the $label without looking up a live execution", async ({ expected, identity, mapIndex }) => {
  const writer: AssetStateStoreLastUpdatedBy = {
    dag_id: "dag",
    kind: "task",
    map_index: mapIndex,
    run_id: "run",
    task_id: "work",
    ...identity,
  };

  vi.spyOn(AssetStateStoreService, "listAssetStateStore").mockResolvedValue({
    asset_state_store: [
      {
        key: "watermark",
        last_updated_by: writer,
        updated_at: "2026-09-28T00:00:00Z",
        value: "ready",
      },
    ],
    total_entries: 1,
  });

  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={["/assets/1/state"]}>
        <TimezoneProvider>
          <Routes>
            <Route element={<AssetStateStore />} path="/assets/:assetId/state" />
          </Routes>
        </TimezoneProvider>
      </MemoryRouter>
    </BaseWrapper>,
  );

  expect(await screen.findByRole("link", { name: "work" })).toHaveAttribute("href", expected);
});
