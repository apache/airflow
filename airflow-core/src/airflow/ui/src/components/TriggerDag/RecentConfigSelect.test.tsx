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
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import RecentConfigSelect from "./RecentConfigSelect";

const buildRun = (overrides: Record<string, unknown>) => ({
  bundle_version: null,
  conf: null,
  dag_display_name: "test_dag",
  dag_id: "test_dag",
  dag_run_id: "run_1",
  dag_versions: [],
  data_interval_end: null,
  data_interval_start: null,
  duration: null,
  end_date: null,
  last_scheduling_decision: null,
  logical_date: null,
  note: null,
  partition_key: null,
  queued_at: null,
  run_after: "2025-01-01T00:00:00Z",
  run_type: "manual" as const,
  start_date: null,
  state: "success" as const,
  triggered_by: "ui" as const,
  triggering_user_name: null,
  ...overrides,
});

let mockData: { dag_runs: Array<ReturnType<typeof buildRun>> } | undefined;
const mockIsLoading = false;

vi.mock("openapi/queries", () => ({
  useDagRunServiceGetDagRuns: vi.fn(() => ({
    data: mockData,
    isLoading: mockIsLoading,
  })),
}));

const getItems = (container: HTMLElement) => container.querySelectorAll(".chakra-select__item");

describe("RecentConfigSelect", () => {
  it("renders one item per distinct non-empty conf", () => {
    mockData = {
      dag_runs: [
        buildRun({ conf: { message: "First" }, dag_run_id: "run_1", run_after: "2025-01-03T00:00:00Z" }),
        buildRun({ conf: { message: "Second" }, dag_run_id: "run_2", run_after: "2025-01-02T00:00:00Z" }),
      ],
    };

    const { container } = render(<RecentConfigSelect dagId="test_dag" onSelectConf={vi.fn()} open />, {
      wrapper: Wrapper,
    });

    expect(getItems(container)).toHaveLength(2);
  });

  it("dedups identical confs", () => {
    mockData = {
      dag_runs: [
        buildRun({ conf: { message: "Same" }, dag_run_id: "run_1", run_after: "2025-01-03T00:00:00Z" }),
        buildRun({ conf: { message: "Same" }, dag_run_id: "run_2", run_after: "2025-01-02T00:00:00Z" }),
      ],
    };

    const { container } = render(<RecentConfigSelect dagId="test_dag" onSelectConf={vi.fn()} open />, {
      wrapper: Wrapper,
    });

    expect(getItems(container)).toHaveLength(1);
  });

  it("excludes null/empty conf runs and renders nothing when none remain", () => {
    mockData = {
      dag_runs: [buildRun({ conf: null, dag_run_id: "run_1" }), buildRun({ conf: {}, dag_run_id: "run_2" })],
    };

    const { container } = render(<RecentConfigSelect dagId="test_dag" onSelectConf={vi.fn()} open />, {
      wrapper: Wrapper,
    });

    expect(container).toBeEmptyDOMElement();
  });

  it("renders all distinct confs without capping the list", () => {
    mockData = {
      dag_runs: Array.from({ length: 8 }, (_unused, index) =>
        buildRun({
          conf: { message: `Message ${index}` },
          dag_run_id: `run_${index}`,
          run_after: `2025-01-${(10 - index).toString().padStart(2, "0")}T00:00:00Z`,
        }),
      ),
    };

    const { container } = render(<RecentConfigSelect dagId="test_dag" onSelectConf={vi.fn()} open />, {
      wrapper: Wrapper,
    });

    expect(getItems(container)).toHaveLength(8);
  });

  it("calls onSelectConf with the selected run's conf", async () => {
    mockData = {
      dag_runs: [buildRun({ conf: { message: "Pick me" }, dag_run_id: "run_1" })],
    };
    const onSelectConf = vi.fn();

    render(<RecentConfigSelect dagId="test_dag" onSelectConf={onSelectConf} open />, { wrapper: Wrapper });

    fireEvent.click(screen.getByRole("combobox"));

    await waitFor(() => expect(screen.getByRole("listbox")).toBeInTheDocument());

    fireEvent.click(screen.getByText("run_1"));

    await waitFor(() => expect(onSelectConf).toHaveBeenCalledWith({ message: "Pick me" }));
  });

  it("displays the selected run id in the trigger after selection", async () => {
    mockData = {
      dag_runs: [buildRun({ conf: { message: "Pick me" }, dag_run_id: "run_1" })],
    };

    render(<RecentConfigSelect dagId="test_dag" onSelectConf={vi.fn()} open />, { wrapper: Wrapper });

    const trigger = screen.getByRole("combobox");

    fireEvent.click(trigger);
    await waitFor(() => expect(screen.getByRole("listbox")).toBeInTheDocument());
    fireEvent.click(screen.getByText("run_1"));

    await waitFor(() => expect(trigger).toHaveTextContent("run_1"));
  });
});
