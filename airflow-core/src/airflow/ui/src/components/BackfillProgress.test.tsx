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
import { render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { BackfillDagRunResponse } from "openapi/requests/types.gen";
import { Wrapper } from "src/utils/Wrapper";

import { BackfillProgress } from "./BackfillProgress";

const mocks = vi.hoisted(() => ({
  listBackfillDagRuns: vi.fn(),
}));

vi.mock("openapi/requests/services.gen", () => ({
  BackfillService: { listBackfillDagRuns: mocks.listBackfillDagRuns },
}));

const dagRun: BackfillDagRunResponse = {
  backfill_id: 7,
  dag_id: "example_dag",
  dag_run_id: "run_1",
  dag_run_state: "success",
  exception_reason: null,
  id: 1,
  logical_date: "2026-07-01T00:00:00Z",
  partition_key: null,
  sort_ordinal: 1,
};

const makeDagRuns = (count: number, start = 0) =>
  Array.from({ length: count }, (_, index): BackfillDagRunResponse => {
    const ordinal = start + index + 1;

    return {
      ...dagRun,
      dag_run_id: `run_${ordinal}`,
      id: ordinal,
      sort_ordinal: ordinal,
    };
  });

describe("BackfillProgress", () => {
  beforeEach(() => mocks.listBackfillDagRuns.mockReset());

  it("renders nothing when there are no progress entries", async () => {
    mocks.listBackfillDagRuns.mockResolvedValue({ backfill_dag_runs: [], total_entries: 0 });

    const { container } = render(<BackfillProgress backfillId={7} isCompleted={false} />, {
      wrapper: Wrapper,
    });

    await waitFor(() => expect(mocks.listBackfillDagRuns).toHaveBeenCalledOnce());
    expect(container).toBeEmptyDOMElement();
  });

  it("fetches completed backfills once without polling", async () => {
    mocks.listBackfillDagRuns.mockResolvedValue({ backfill_dag_runs: [dagRun], total_entries: 1 });

    render(<BackfillProgress backfillId={7} isCompleted refetchInterval={10} />, { wrapper: Wrapper });

    expect(await screen.findByText("1/1")).toBeInTheDocument();
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(mocks.listBackfillDagRuns).toHaveBeenCalledOnce();
  });

  it("polls active backfills at the requested interval", async () => {
    mocks.listBackfillDagRuns.mockResolvedValue({ backfill_dag_runs: [dagRun], total_entries: 1 });

    render(<BackfillProgress backfillId={7} isCompleted={false} refetchInterval={10} />, {
      wrapper: Wrapper,
    });

    expect(await screen.findByText("1/1")).toBeInTheDocument();
    await waitFor(() => expect(mocks.listBackfillDagRuns.mock.calls.length).toBeGreaterThan(1), {
      timeout: 1000,
    });
  });

  it("counts completed runs beyond the first API page", async () => {
    mocks.listBackfillDagRuns.mockImplementation(({ offset }: { offset?: number } = {}) => {
      if (offset === 100) {
        return Promise.resolve({ backfill_dag_runs: makeDagRuns(1, 100), total_entries: 101 });
      }

      return Promise.resolve({ backfill_dag_runs: makeDagRuns(100), total_entries: 101 });
    });

    render(<BackfillProgress backfillId={7} isCompleted />, { wrapper: Wrapper });

    expect(await screen.findByText("101/101")).toBeInTheDocument();
    expect(mocks.listBackfillDagRuns).toHaveBeenNthCalledWith(1, {
      backfillId: 7,
      limit: 100,
      offset: 0,
    });
    expect(mocks.listBackfillDagRuns).toHaveBeenNthCalledWith(2, {
      backfillId: 7,
      limit: 100,
      offset: 100,
    });
  });

  it("does not show progress from a previously rendered backfill", async () => {
    mocks.listBackfillDagRuns.mockImplementation(({ backfillId }: { backfillId?: number } = {}) => {
      if (backfillId === 8) {
        return new Promise(() => undefined);
      }

      return Promise.resolve({ backfill_dag_runs: [dagRun], total_entries: 1 });
    });

    const { rerender } = render(<BackfillProgress backfillId={7} isCompleted />, { wrapper: Wrapper });

    expect(await screen.findByText("1/1")).toBeInTheDocument();

    rerender(<BackfillProgress backfillId={8} isCompleted />);

    await waitFor(() =>
      expect(mocks.listBackfillDagRuns).toHaveBeenLastCalledWith({ backfillId: 8, limit: 100, offset: 0 }),
    );
    expect(screen.queryByText("1/1")).not.toBeInTheDocument();
  });
});
