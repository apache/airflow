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
import { fireEvent, render, screen } from "@testing-library/react";
import type React from "react";
import { describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import RunBackfillForm from "./RunBackfillForm";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    // eslint-disable-next-line id-length
    t: (key: string, opts?: { count?: number; dag_display_name?: string }) => {
      const map: Record<string, string> = {
        "backfill.affected": `${String(opts?.count)} runs will be triggered.`,
        "backfill.affectedNone": "No runs matching selected criteria.",
        "backfill.andOthers": `…and ${String(opts?.count)} more`,
        "backfill.backwards": "Run Backwards",
        "backfill.dateRange": "Date Range",
        "backfill.maxRuns": "Max Active Runs",
        "backfill.overrideExistingParams": "Override parameters on existing runs",
        "backfill.partitionRange": "Partition Range",
        "backfill.partitionsAffected": `${String(opts?.count)} partitions will be backfilled:`,
        "backfill.partitionsNone": "No partitions matching selected range.",
        "backfill.reprocessBehavior": "Reprocess Behavior",
        "backfill.run": "Run Backfill",
        "backfill.schedulerPriorityHint": "Scheduler priority hint",
        "common:modal.cancel": "Cancel",
        "common:table.from": "From",
        "common:table.to": "To",
        "dags:runAndTaskActions.options.runOnLatestVersion": "Run on latest version",
      };

      return map[key] ?? key;
    },
  }),
}));

vi.mock("openapi/queries", () => ({
  useDagServiceGetDagDetails: vi.fn(() => ({ data: undefined })),
}));

vi.mock("src/queries/useCreateBackfillDryRun", () => ({
  useCreateBackfillDryRun: vi.fn(() => ({
    data: undefined,
    error: undefined,
    isPending: false,
  })),
}));

vi.mock("src/queries/useCreateBackfill", () => ({
  useCreateBackfill: vi.fn(() => ({
    createBackfill: vi.fn(),
    dateValidationError: undefined,
    error: undefined,
    isPending: false,
  })),
}));

vi.mock("src/queries/useDagParams", () => ({
  useDagParams: vi.fn(() => ({ paramsDict: {} })),
}));

vi.mock("src/queries/useParamStore", () => ({
  useParamStore: vi.fn(() => ({ conf: "{}" })),
}));

vi.mock("src/queries/useTogglePause", () => ({
  useTogglePause: vi.fn(() => ({ mutate: vi.fn() })),
}));

vi.mock("src/components/Clear/useRerunWithLatestVersion", () => ({
  useRerunWithLatestVersion: vi.fn(() => ({ value: false })),
}));

vi.mock("../DateTimeInput", () => ({
  DateTimeInput: ({
    onChange,
    value = "",
  }: {
    readonly onChange?: React.ChangeEventHandler<HTMLInputElement>;
    readonly value?: string;
  }) => <input aria-label="datetime" onChange={onChange} value={value} />,
}));

vi.mock("../ConfigForm", () => ({
  default: () => <div data-testid="config-form" />,
}));

const baseDag = {
  dag_display_name: "Test Dag",
  dag_id: "test_dag",
  is_paused: false,
  max_active_runs: 10,
  timetable_partitioned: false,
};

const { useDagServiceGetDagDetails } = await import("openapi/queries");
const { useCreateBackfillDryRun } = await import("src/queries/useCreateBackfillDryRun");

describe("RunBackfillForm", () => {
  it("shows 'Date Range' label for non-partitioned Dags", () => {
    vi.mocked(useDagServiceGetDagDetails).mockReturnValue({
      data: { ...baseDag, timetable_partitioned: false },
    } as ReturnType<typeof useDagServiceGetDagDetails>);

    render(<RunBackfillForm dag={baseDag as never} onClose={vi.fn()} />, { wrapper: Wrapper });

    expect(screen.getByText("Date Range")).toBeInTheDocument();
    expect(screen.queryByText("Partition Range")).not.toBeInTheDocument();
  });

  it("shows 'Partition Range' label for partitioned Dags", () => {
    vi.mocked(useDagServiceGetDagDetails).mockReturnValue({
      data: { ...baseDag, timetable_partitioned: true },
    } as ReturnType<typeof useDagServiceGetDagDetails>);

    render(<RunBackfillForm dag={{ ...baseDag, timetable_partitioned: true } as never} onClose={vi.fn()} />, {
      wrapper: Wrapper,
    });

    expect(screen.getByText("Partition Range")).toBeInTheDocument();
    expect(screen.queryByText("Date Range")).not.toBeInTheDocument();
  });

  it("disables Run button when dry-run returns 0 entries", () => {
    vi.mocked(useDagServiceGetDagDetails).mockReturnValue({
      data: { ...baseDag, timetable_partitioned: true },
    } as ReturnType<typeof useDagServiceGetDagDetails>);

    vi.mocked(useCreateBackfillDryRun).mockReturnValue({
      data: { backfills: [], total_entries: 0 },
      error: undefined,
      isPending: false,
    } as ReturnType<typeof useCreateBackfillDryRun>);

    render(<RunBackfillForm dag={{ ...baseDag, timetable_partitioned: true } as never} onClose={vi.fn()} />, {
      wrapper: Wrapper,
    });

    expect(screen.getByText("Run Backfill")).toBeDisabled();
  });

  it("does not disable Run button when dry-run returns entries and dates are filled", () => {
    vi.mocked(useDagServiceGetDagDetails).mockReturnValue({
      data: { ...baseDag, timetable_partitioned: false },
    } as ReturnType<typeof useDagServiceGetDagDetails>);

    vi.mocked(useCreateBackfillDryRun).mockReturnValue({
      data: {
        backfills: [{ logical_date: "2024-01-01T00:00:00Z", partition_date: null, partition_key: null }],
        total_entries: 1,
      },
      error: undefined,
      isPending: false,
    } as ReturnType<typeof useCreateBackfillDryRun>);

    render(<RunBackfillForm dag={baseDag as never} onClose={vi.fn()} />, { wrapper: Wrapper });

    expect(screen.getByText("Run Backfill")).not.toBeDisabled();
  });

  it("renders partition key list when dates are filled and dry-run returns partitioned data", () => {
    vi.mocked(useDagServiceGetDagDetails).mockReturnValue({
      data: { ...baseDag, timetable_partitioned: true },
    } as ReturnType<typeof useDagServiceGetDagDetails>);

    vi.mocked(useCreateBackfillDryRun).mockReturnValue({
      data: {
        backfills: [
          { logical_date: null, partition_date: null, partition_key: "2024-01-01T00/2024-01-02T00" },
          { logical_date: null, partition_date: null, partition_key: "2024-01-02T00/2024-01-03T00" },
        ],
        total_entries: 2,
      },
      error: undefined,
      isPending: false,
    } as ReturnType<typeof useCreateBackfillDryRun>);

    render(<RunBackfillForm dag={{ ...baseDag, timetable_partitioned: true } as never} onClose={vi.fn()} />, {
      wrapper: Wrapper,
    });

    const [fromInput, toInput] = screen.getAllByLabelText("datetime");

    fireEvent.change(fromInput as HTMLElement, { target: { value: "2024-01-01T00:00" } });
    fireEvent.change(toInput as HTMLElement, { target: { value: "2024-01-03T00:00" } });

    expect(screen.getByText("2 partitions will be backfilled:")).toBeInTheDocument();
    expect(screen.getByText("2024-01-01T00/2024-01-02T00")).toBeInTheDocument();
    expect(screen.getByText("2024-01-02T00/2024-01-03T00")).toBeInTheDocument();
  });
});
