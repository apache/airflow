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
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";
import type { PartitionedDagRunResponse } from "openapi/requests/types.gen";
import type { TableState } from "src/components/DataTable/types";
import type * as Ui from "src/components/ui";
import { Wrapper } from "src/utils/Wrapper";

import { PartitionScheduleModal } from "./PartitionScheduleModal";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    // eslint-disable-next-line id-length
    t: (key: string, options?: { count?: number }) =>
      options?.count === undefined ? key : `${key}:${options.count}`,
  }),
}));

vi.mock("src/queries/useConfig", () => ({
  useConfig: () => 2,
}));

vi.mock("src/components/ui", async (importOriginal) => {
  const actual = await importOriginal<typeof Ui>();
  // Must stay inside the factory: vitest hoists vi.mock above module scope, so an outer-scope
  // component cannot be referenced here.
  // eslint-disable-next-line unicorn/consistent-function-scoping
  const DialogPart = ({ children }: { readonly children?: ReactNode }) => <div>{children}</div>;

  return {
    ...actual,
    Dialog: {
      ...actual.Dialog,
      Body: DialogPart,
      CloseTrigger: () => undefined,
      Content: DialogPart,
      Header: DialogPart,
      Root: ({
        children,
        onOpenChange,
        open,
      }: {
        readonly children?: ReactNode;
        readonly onOpenChange?: () => void;
        readonly open?: boolean;
      }) =>
        open ? (
          <div>
            <button onClick={onOpenChange} type="button">
              close dialog
            </button>
            {children}
          </div>
        ) : undefined,
    },
  };
});

vi.mock("src/components/DataTable", () => ({
  DataTable: ({
    data,
    initialState,
    onStateChange,
    total,
  }: {
    readonly data: ReadonlyArray<PartitionedDagRunResponse>;
    readonly initialState?: TableState;
    readonly onStateChange?: (state: TableState) => void;
    readonly total?: number;
  }) => (
    <div>
      <div data-testid="partitioned-dag-run-count">{data.length}</div>
      <div data-testid="partitioned-dag-run-total">{total}</div>
      <button
        onClick={() => {
          if (initialState !== undefined) {
            onStateChange?.({
              ...initialState,
              pagination: {
                ...initialState.pagination,
                pageIndex: 1,
              },
            });
          }
        }}
        type="button"
      >
        next page
      </button>
    </div>
  ),
}));

vi.mock("openapi/queries", async (importOriginal) => {
  const actual = await importOriginal<typeof OpenapiQueries>();

  return {
    ...actual,
    usePartitionedDagRunServiceGetPartitionedDagRuns: vi.fn(),
  };
});

const { usePartitionedDagRunServiceGetPartitionedDagRuns } = await import("openapi/queries");

const partitionedDagRun = {
  dag_id: "example_dag",
  id: 1,
  partition_key: "2024-06-01",
  total_received: 0,
  total_required: 1,
} satisfies PartitionedDagRunResponse;

const getPartitionedDagRunsResponse = (partitionedDagRuns: Array<PartitionedDagRunResponse>) =>
  ({
    data: {
      asset_expressions: null,
      partitioned_dag_runs: partitionedDagRuns,
      total: 3,
    },
    error: null,
    isFetching: false,
    isLoading: false,
  }) as ReturnType<typeof usePartitionedDagRunServiceGetPartitionedDagRuns>;

describe("PartitionScheduleModal", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(usePartitionedDagRunServiceGetPartitionedDagRuns).mockReturnValue(
      getPartitionedDagRunsResponse([partitionedDagRun]),
    );
  });

  it("requests partitioned Dag runs for the current table page", async () => {
    const onClose = vi.fn();

    render(<PartitionScheduleModal dagId="example_dag" onClose={onClose} open />, { wrapper: Wrapper });

    expect(usePartitionedDagRunServiceGetPartitionedDagRuns).toHaveBeenLastCalledWith(
      {
        dagId: "example_dag",
        hasCreatedDagRunId: false,
        limit: 2,
        offset: 0,
      },
      undefined,
      { enabled: true },
    );
    expect(screen.getByTestId("partitioned-dag-run-total")).toHaveTextContent("3");

    fireEvent.click(screen.getByRole("button", { name: "next page" }));

    await waitFor(() =>
      expect(usePartitionedDagRunServiceGetPartitionedDagRuns).toHaveBeenLastCalledWith(
        {
          dagId: "example_dag",
          hasCreatedDagRunId: false,
          limit: 2,
          offset: 2,
        },
        undefined,
        { enabled: true },
      ),
    );

    fireEvent.click(screen.getByRole("button", { name: "close dialog" }));

    expect(onClose).toHaveBeenCalledTimes(1);
    await waitFor(() =>
      expect(usePartitionedDagRunServiceGetPartitionedDagRuns).toHaveBeenLastCalledWith(
        {
          dagId: "example_dag",
          hasCreatedDagRunId: false,
          limit: 2,
          offset: 0,
        },
        undefined,
        { enabled: true },
      ),
    );
  });
});
