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
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { PoolResponse } from "openapi/requests/types.gen";
import { Wrapper } from "src/utils/Wrapper";

import { PoolSummary } from "./PoolSummary";

const mocks = vi.hoisted(() => ({
  useAuthLinksServiceGetAuthMenus: vi.fn(),
  usePoolServiceGetPools: vi.fn(),
}));

vi.mock("openapi/queries", () => ({
  useAuthLinksServiceGetAuthMenus: mocks.useAuthLinksServiceGetAuthMenus,
}));

vi.mock("openapi/queries/queries", () => ({
  usePoolServiceGetPools: mocks.usePoolServiceGetPools,
}));

vi.mock("src/utils", () => ({
  useAutoRefresh: () => false,
}));

const createPool = (pool: Partial<PoolResponse>): PoolResponse => ({
  deferred_slots: 1,
  description: null,
  include_deferred: false,
  name: "default_pool",
  occupied_slots: 1,
  open_slots: 128,
  queued_slots: 0,
  running_slots: 0,
  scheduled_slots: 0,
  slots: 128,
  team_name: null,
  ...pool,
});

const renderPoolSummary = (pools: Array<PoolResponse>) => {
  mocks.useAuthLinksServiceGetAuthMenus.mockReturnValue({
    data: { authorized_menu_items: ["Pools"] },
  });
  mocks.usePoolServiceGetPools.mockReturnValue({
    data: { pools, total_entries: pools.length },
    error: undefined,
    isLoading: false,
  });

  return render(<PoolSummary />, { wrapper: Wrapper });
};

describe("PoolSummary", () => {
  beforeEach(() => {
    mocks.useAuthLinksServiceGetAuthMenus.mockReset();
    mocks.usePoolServiceGetPools.mockReset();
  });

  it("does not aggregate non-consuming deferred slots into the dashboard usage bar", () => {
    renderPoolSummary([createPool({ include_deferred: false })]);

    expect(screen.getByText("128")).toBeInTheDocument();
    expect(screen.queryByText("1")).not.toBeInTheDocument();
  });

  it("aggregates consuming deferred slots into the dashboard usage bar", () => {
    renderPoolSummary([createPool({ include_deferred: true, open_slots: 127 })]);

    expect(screen.getByText("127")).toBeInTheDocument();
    expect(screen.getByText("1")).toBeInTheDocument();
  });

  it("aggregates deferred slots only from pools that include them", () => {
    renderPoolSummary([
      createPool({ include_deferred: false }),
      createPool({ include_deferred: true, name: "counting_pool", open_slots: 127 }),
    ]);

    expect(screen.getByText("255")).toBeInTheDocument();
    expect(screen.getByText("1")).toBeInTheDocument();
    expect(screen.queryByText("2")).not.toBeInTheDocument();
  });
});
