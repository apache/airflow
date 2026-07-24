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
import type { ReactNode } from "react";
import type * as ReactRouterDom from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { BackfillResponse } from "openapi/requests/types.gen";
import type * as Utils from "src/utils";
import { Wrapper } from "src/utils/Wrapper";

import { Backfill } from "./Backfill";

const mocks = vi.hoisted(() => ({
  useBackfillServiceGetBackfill: vi.fn(),
  useParams: vi.fn(),
}));

vi.mock("openapi/queries", () => ({
  useBackfillServiceGetBackfill: mocks.useBackfillServiceGetBackfill,
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    // eslint-disable-next-line id-length
    t: (key: string) => key,
  }),
}));

vi.mock("react-router-dom", async (importOriginal) => {
  const actual = await importOriginal<typeof ReactRouterDom>();

  return { ...actual, useParams: mocks.useParams };
});

vi.mock("src/layouts/Details/DetailsLayout", () => ({
  DetailsLayout: ({
    children,
    outletContext,
    showBackfillBanner,
  }: {
    readonly children: ReactNode;
    readonly outletContext?: BackfillResponse;
    readonly showBackfillBanner?: boolean;
  }) => (
    <div>
      {children}
      <span>{outletContext?.dag_id}</span>
      <span>{`show-banner-${String(showBackfillBanner)}`}</span>
    </div>
  ),
}));

vi.mock("./Header", () => ({
  Header: () => <div>backfill header</div>,
}));

vi.mock("src/queries/useConfig", () => ({
  useConfig: () => undefined,
}));

vi.mock("src/utils", async (importOriginal) => {
  const actual = await importOriginal<typeof Utils>();

  return { ...actual, useAutoRefresh: () => 5000 };
});

const backfill: BackfillResponse = {
  completed_at: null,
  created_at: "2026-07-01T00:00:00Z",
  dag_display_name: "Example Dag",
  dag_id: "example_dag",
  dag_run_conf: null,
  from_date: "2026-07-01T00:00:00Z",
  id: 7,
  is_paused: false,
  max_active_runs: 4,
  reprocess_behavior: "failed",
  to_date: "2026-07-05T00:00:00Z",
  updated_at: "2026-07-01T00:00:00Z",
};

describe("Backfill page", () => {
  beforeEach(() => {
    mocks.useBackfillServiceGetBackfill.mockReset();
    mocks.useParams.mockReturnValue({ backfillId: "7", dagId: "example_dag" });
  });

  it("loads the requested backfill and renders its header", () => {
    mocks.useBackfillServiceGetBackfill.mockReturnValue({
      data: backfill,
      error: undefined,
      isLoading: false,
    });

    render(<Backfill />, { wrapper: Wrapper });

    const [parameters, queryKey, options] = mocks.useBackfillServiceGetBackfill.mock.calls[0] as [
      { backfillId: number },
      undefined,
      {
        enabled: boolean;
        refetchInterval: (query: { state: { data?: BackfillResponse } }) => number | false;
      },
    ];

    expect(parameters).toEqual({ backfillId: 7 });
    expect(queryKey).toBeUndefined();
    expect(options.enabled).toBe(true);
    expect(options.refetchInterval({ state: { data: backfill } })).toBe(5000);
    expect(
      options.refetchInterval({
        state: { data: { ...backfill, completed_at: "2026-07-02T00:00:00Z" } },
      }),
    ).toBe(false);
    expect(screen.getByText("backfill header")).toBeInTheDocument();
    expect(screen.getByText("example_dag")).toBeInTheDocument();
    expect(screen.getByText("show-banner-false")).toBeInTheDocument();
  });

  it("does not render a header while the response is pending", () => {
    mocks.useBackfillServiceGetBackfill.mockReturnValue({
      data: undefined,
      error: undefined,
      isLoading: true,
    });

    render(<Backfill />, { wrapper: Wrapper });

    expect(screen.queryByText("backfill header")).not.toBeInTheDocument();
  });

  it("does not request an invalid backfill ID", () => {
    mocks.useParams.mockReturnValue({ backfillId: "not-a-number", dagId: "example_dag" });
    mocks.useBackfillServiceGetBackfill.mockReturnValue({
      data: undefined,
      error: undefined,
      isLoading: false,
    });

    render(<Backfill />, { wrapper: Wrapper });

    expect(mocks.useBackfillServiceGetBackfill).toHaveBeenCalledWith(
      { backfillId: 0 },
      undefined,
      expect.objectContaining({ enabled: false }),
    );
  });
});
