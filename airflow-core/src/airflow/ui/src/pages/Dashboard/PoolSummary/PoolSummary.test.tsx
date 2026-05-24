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
import { beforeEach, describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";
import type { PoolResponse } from "openapi/requests/types.gen";
import type * as SrcUtils from "src/utils";
import { Wrapper } from "src/utils/Wrapper";

import { PoolSummary } from "./PoolSummary";

vi.mock("react-i18next", async (importOriginal) => {
  const actual = await importOriginal<typeof ReactI18Next>();

  return {
    ...actual,
    useTranslation: () => ({
      // eslint-disable-next-line id-length
      t: (key: string) => key,
    }),
  };
});

vi.mock("openapi/queries", async (importOriginal) => {
  const actual = await importOriginal<typeof OpenapiQueries>();

  return {
    ...actual,
    useAuthLinksServiceGetAuthMenus: vi.fn(),
  };
});

vi.mock("openapi/queries/queries", () => ({
  usePoolServiceGetPools: vi.fn(),
}));

vi.mock("src/utils", async (importOriginal) => {
  const actual = await importOriginal<typeof SrcUtils>();

  return {
    ...actual,
    useAutoRefresh: () => false,
  };
});

const { useAuthLinksServiceGetAuthMenus } = await import("openapi/queries");
const { usePoolServiceGetPools } = await import("openapi/queries/queries");

const makePool = (overrides: Partial<PoolResponse>): PoolResponse => ({
  deferred_slots: 0,
  description: null,
  include_deferred: false,
  name: "default_pool",
  occupied_slots: 0,
  open_slots: 128,
  queued_slots: 0,
  running_slots: 0,
  scheduled_slots: 0,
  slots: 128,
  team_name: null,
  ...overrides,
});

const renderPoolSummary = (pools: Array<PoolResponse>) => {
  vi.mocked(usePoolServiceGetPools).mockReturnValue({
    data: { pools, total_entries: pools.length },
    error: null,
    isLoading: false,
  } as ReturnType<typeof usePoolServiceGetPools>);

  render(<PoolSummary />, { wrapper: Wrapper });
};

beforeEach(() => {
  vi.mocked(useAuthLinksServiceGetAuthMenus).mockReturnValue({
    data: { authorized_menu_items: ["Pools"] },
  } as ReturnType<typeof useAuthLinksServiceGetAuthMenus>);
});

describe("PoolSummary", () => {
  it("does not include deferred tasks in the chart when they do not consume pool slots", () => {
    renderPoolSummary([makePool({ deferred_slots: 1, include_deferred: false })]);

    expect(screen.getByText("128")).toBeInTheDocument();
    expect(screen.queryByText("1")).not.toBeInTheDocument();
  });

  it("includes deferred tasks in the chart when they consume pool slots", () => {
    renderPoolSummary([
      makePool({ deferred_slots: 1, include_deferred: true, occupied_slots: 1, open_slots: 127 }),
    ]);

    expect(screen.getByText("127")).toBeInTheDocument();
    expect(screen.getByText("1")).toBeInTheDocument();
  });
});
