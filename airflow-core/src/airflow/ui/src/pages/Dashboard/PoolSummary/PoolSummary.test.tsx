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
import { act, fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { PoolResponse } from "openapi/requests/types.gen";
import { Wrapper } from "src/utils/Wrapper";

import { PoolSummary } from "./PoolSummary";

const mocks = vi.hoisted(() => ({
  deferredSlotsNotCountedTooltip:
    "Deferred tasks shown in the bar are counted against pool slots. " +
    "Deferred tasks shown below the bar are from pools that do not count deferred tasks against slots.",
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

vi.mock("react-i18next", () => ({
  useTranslation: (namespace: string) => ({
    // eslint-disable-next-line id-length
    t: (key: string, options?: { count?: number }) => {
      if (namespace === "dashboard" && key === "deferredSlotsNotCounted") {
        return `Deferred not counted in slots: ${options?.count}`;
      }

      if (namespace === "dashboard" && key === "deferredSlotsNotCountedTooltip") {
        return mocks.deferredSlotsNotCountedTooltip;
      }

      return `${namespace}:${key}`;
    },
  }),
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

  it("shows non-consuming deferred slots below the dashboard usage bar", () => {
    renderPoolSummary([createPool({ deferred_slots: 32, include_deferred: false })]);

    expect(screen.getByText("128")).toBeInTheDocument();
    expect(screen.queryByText("32")).not.toBeInTheDocument();
    expect(screen.getByText("Deferred not counted in slots: 32")).toBeInTheDocument();
  });

  it("aggregates consuming deferred slots into the dashboard usage bar", () => {
    renderPoolSummary([createPool({ include_deferred: true, open_slots: 127 })]);

    expect(screen.getByText("127")).toBeInTheDocument();
    expect(screen.getByText("1")).toBeInTheDocument();
    expect(screen.queryByText(/Deferred not counted in slots/u)).not.toBeInTheDocument();
  });

  it("shows both counted and non-counted deferred slots for mixed pools", () => {
    renderPoolSummary([
      createPool({ deferred_slots: 32, include_deferred: false }),
      createPool({ deferred_slots: 3, include_deferred: true, name: "counting_pool", open_slots: 125 }),
    ]);

    expect(screen.getByText("253")).toBeInTheDocument();
    expect(screen.getByText("3")).toBeInTheDocument();
    expect(screen.queryByText("35")).not.toBeInTheDocument();
    expect(screen.getByText("Deferred not counted in slots: 32")).toBeInTheDocument();
  });

  it("explains the dashboard deferred slot distinction in a tooltip", async () => {
    vi.useFakeTimers();

    renderPoolSummary([createPool({ deferred_slots: 32, include_deferred: false })]);

    const deferredSlotsInfo = screen
      .getByText("Deferred not counted in slots: 32")
      .closest('[data-part="trigger"]');

    expect(deferredSlotsInfo).not.toBeNull();

    try {
      await act(async () => {
        fireEvent.focus(deferredSlotsInfo as Element);
        fireEvent.pointerEnter(deferredSlotsInfo as Element);
        await vi.advanceTimersByTimeAsync(500);
      });

      expect(screen.getByText(mocks.deferredSlotsNotCountedTooltip)).toBeInTheDocument();
    } finally {
      vi.useRealTimers();
    }
  });
});
