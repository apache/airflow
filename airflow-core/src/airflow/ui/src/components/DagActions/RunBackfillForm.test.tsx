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
import { fireEvent, render, screen } from "@testing-library/react";
import type { DAGWithLatestDagRunsResponse } from "openapi-gen/requests/types.gen";
import type { PropsWithChildren } from "react";
import type * as ReactI18Next from "react-i18next";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { TimezoneProvider } from "src/context/timezone";
import { useCreateBackfill } from "src/queries/useCreateBackfill";
import { useCreateBackfillDryRun } from "src/queries/useCreateBackfillDryRun";
import { useDagParams } from "src/queries/useDagParams";
import { useParamStore } from "src/queries/useParamStore";
import { useTogglePause } from "src/queries/useTogglePause";
import { BaseWrapper } from "src/utils/Wrapper";

import RunBackfillForm from "./RunBackfillForm";

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

// Mock the timezone context to avoid localStorage access (same pattern as DagCard.test.tsx)
vi.mock("src/context/timezone", async () => {
  const actual = await vi.importActual("src/context/timezone");

  return {
    ...actual,
    TimezoneProvider: ({ children }: PropsWithChildren) => children,
    useTimezone: () => ({
      selectedTimezone: "UTC",
      setSelectedTimezone: vi.fn(),
    }),
  };
});

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

// Mock openapi/queries to intercept network calls
vi.mock("openapi/queries", () => ({
  useBackfillServiceListBackfillsUiKey: "useBackfillServiceListBackfillsUiKey",
  useConfigServiceGetConfigs: vi.fn(),
  useDagServiceGetDagDetails: vi.fn(),
}));

// Mock DateTimeInput with a plain <input> so fireEvent.change updates RHF form state immediately
vi.mock("src/components/DateTimeInput", () => ({
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  DateTimeInput: vi.fn(({ onChange, value, ...rest }: any) => (
    // eslint-disable-next-line react/jsx-props-no-spreading
    <input
      {...rest}
      data-testid="datetime-input"
      onChange={(event) => {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-call
        onChange?.({ ...event, target: { ...event.target, value: event.target.value } });
      }}
      type="datetime-local"
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      value={value}
    />
  )),
}));

vi.mock("src/queries/useCreateBackfill", () => ({
  useCreateBackfill: vi.fn(),
}));

vi.mock("src/queries/useCreateBackfillDryRun", () => ({
  useCreateBackfillDryRun: vi.fn(),
}));

vi.mock("src/queries/useDagParams", () => ({
  useDagParams: vi.fn(),
}));

vi.mock("src/queries/useParamStore", () => ({
  useParamStore: vi.fn(),
}));

vi.mock("src/queries/useTogglePause", () => ({
  useTogglePause: vi.fn(),
}));

// ---------------------------------------------------------------------------
// Mock dag fixtures (based on DagCard.test.tsx:111 convention)
// ---------------------------------------------------------------------------

const baseMockDag: DAGWithLatestDagRunsResponse = {
  allowed_run_types: null,
  asset_expression: null,
  bundle_name: "dags-folder",
  bundle_version: "1",
  dag_display_name: "test_dag",
  dag_id: "test_dag",
  description: null,
  file_token: "test-token",
  fileloc: "/files/dags/test_dag.py",
  has_import_errors: false,
  has_task_concurrency_limits: false,
  is_backfillable: true,
  is_favorite: false,
  is_paused: false,
  is_stale: false,
  last_expired: null,
  last_parse_duration: 0.1,
  last_parsed_time: "2024-08-22T13:50:10Z",
  latest_dag_runs: [],
  max_active_runs: 16,
  max_active_tasks: 16,
  max_consecutive_failed_dag_runs: 0,
  next_dagrun_data_interval_end: null,
  next_dagrun_data_interval_start: null,
  next_dagrun_logical_date: null,
  next_dagrun_run_after: null,
  owners: ["airflow"],
  pending_actions: [],
  relative_fileloc: "test_dag.py",
  tags: [],
  timetable_description: "Every day",
  timetable_partitioned: false,
  timetable_periodic: true,
  timetable_summary: "@daily",
};

const partitionedDag: DAGWithLatestDagRunsResponse = {
  ...baseMockDag,
  timetable_partitioned: true,
};

const nonPartitionedDag: DAGWithLatestDagRunsResponse = {
  ...baseMockDag,
  timetable_partitioned: false,
};

// Custom wrapper: TimezoneProvider is already mocked to pass-through children
const TestWrapper = ({ children }: PropsWithChildren) => (
  <BaseWrapper>
    <MemoryRouter>
      <TimezoneProvider>{children}</TimezoneProvider>
    </MemoryRouter>
  </BaseWrapper>
);

// ---------------------------------------------------------------------------
// Setup / teardown — re-set mock return values every test
// (vite.config mockReset:true clears implementations between tests)
// ---------------------------------------------------------------------------

let mockCreateBackfill: ReturnType<typeof vi.fn>;

const { useConfigServiceGetConfigs, useDagServiceGetDagDetails } = vi.mocked(await import("openapi/queries"));

beforeEach(() => {
  mockCreateBackfill = vi.fn();

  vi.mocked(useCreateBackfill).mockReturnValue({
    createBackfill: mockCreateBackfill,
    dateValidationError: undefined,
    error: undefined,
    isPending: false,
  });

  vi.mocked(useCreateBackfillDryRun).mockReturnValue({
    data: { backfills: [], total_entries: 1 },
    error: undefined,
    isPending: false,
  } as ReturnType<typeof useCreateBackfillDryRun>);

  vi.mocked(useDagParams).mockReturnValue({ paramsDict: {} });

  vi.mocked(useParamStore).mockReturnValue({
    conf: "{}",
    paramsDict: {},
    setConf: vi.fn(),
    setDisabled: vi.fn(),
    setInitialParamDict: vi.fn(),
    setParamsDict: vi.fn(),
  } as unknown as ReturnType<typeof useParamStore>);

  vi.mocked(useTogglePause).mockReturnValue({
    mutate: vi.fn(),
  } as unknown as ReturnType<typeof useTogglePause>);

  useConfigServiceGetConfigs.mockReturnValue({ data: undefined });
  useDagServiceGetDagDetails.mockReturnValue({ data: undefined });
});

afterEach(() => {
  vi.clearAllMocks();
});

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("RunBackfillForm — field rendering", () => {
  it("renders partition date fields and hides from/to date fields for a partitioned Dag", () => {
    render(<RunBackfillForm dag={partitionedDag} isPartitioned={true} onClose={vi.fn()} />, {
      wrapper: TestWrapper,
    });

    expect(screen.getByText("backfill.partitionDateStart")).toBeInTheDocument();
    expect(screen.getByText("backfill.partitionDateEnd")).toBeInTheDocument();
    expect(screen.queryByText("common:table.from")).not.toBeInTheDocument();
    expect(screen.queryByText("common:table.to")).not.toBeInTheDocument();
  });

  it("renders from/to date fields and hides partition date fields for a non-partitioned Dag", () => {
    render(<RunBackfillForm dag={nonPartitionedDag} isPartitioned={false} onClose={vi.fn()} />, {
      wrapper: TestWrapper,
    });

    expect(screen.getByText("common:table.from")).toBeInTheDocument();
    expect(screen.getByText("common:table.to")).toBeInTheDocument();
    expect(screen.queryByText("backfill.partitionDateStart")).not.toBeInTheDocument();
    expect(screen.queryByText("backfill.partitionDateEnd")).not.toBeInTheDocument();
  });
});

describe("RunBackfillForm — form submission body", () => {
  // Mutual-exclusion at the mutate layer (partition vs. from/to) is verified in
  // useCreateBackfill.test.ts; these tests pin the exact raw form-data shape
  // that the component passes to createBackfill.

  it("passes partition date fields (with from/to empty) to createBackfill for a partitioned Dag", async () => {
    render(<RunBackfillForm dag={partitionedDag} isPartitioned={true} onClose={vi.fn()} />, {
      wrapper: TestWrapper,
    });

    // inputs[0] = partition_date_start, inputs[1] = partition_date_end
    const inputs = screen.getAllByTestId("datetime-input");

    fireEvent.change(inputs[0], { target: { value: "2024-01-01T00:00" } });
    fireEvent.change(inputs[1], { target: { value: "2024-01-31T00:00" } });

    fireEvent.click(screen.getByText("backfill.run"));

    await vi.waitFor(() => {
      expect(mockCreateBackfill).toHaveBeenCalledTimes(1);
    });

    expect(mockCreateBackfill).toHaveBeenCalledWith({
      requestBody: {
        conf: "{}",
        dag_id: "test_dag",
        dag_run_conf: null,
        from_date: "",
        max_active_runs: 1,
        partition_date_end: "2024-01-31T00:00",
        partition_date_start: "2024-01-01T00:00",
        reprocess_behavior: "none",
        run_backwards: false,
        run_on_latest_version: true,
        to_date: "",
      },
    });
  });

  it("passes from/to date fields (with partition fields empty) to createBackfill for a non-partitioned Dag", async () => {
    render(<RunBackfillForm dag={nonPartitionedDag} isPartitioned={false} onClose={vi.fn()} />, {
      wrapper: TestWrapper,
    });

    // inputs[0] = from_date, inputs[1] = to_date
    const inputs = screen.getAllByTestId("datetime-input");

    fireEvent.change(inputs[0], { target: { value: "2024-01-01T00:00" } });
    fireEvent.change(inputs[1], { target: { value: "2024-01-31T00:00" } });

    fireEvent.click(screen.getByText("backfill.run"));

    await vi.waitFor(() => {
      expect(mockCreateBackfill).toHaveBeenCalledTimes(1);
    });

    expect(mockCreateBackfill).toHaveBeenCalledWith({
      requestBody: {
        conf: "{}",
        dag_id: "test_dag",
        dag_run_conf: null,
        from_date: "2024-01-01T00:00",
        max_active_runs: 1,
        partition_date_end: "",
        partition_date_start: "",
        reprocess_behavior: "none",
        run_backwards: false,
        run_on_latest_version: true,
        to_date: "2024-01-31T00:00",
      },
    });
  });
});
