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
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import MarkRunAsButton from "./MarkRunAsButton";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    // eslint-disable-next-line id-length
    t: (key: string) => key,
  }),
}));

vi.mock("src/queries/usePatchDagRun", () => ({
  usePatchDagRun: vi.fn(() => ({ isPending: false, mutate: vi.fn() })),
}));

const makeDagRun = (state: "failed" | "success") => ({
  bundle_version: null,
  conf: null,
  dag_display_name: "test_dag",
  dag_id: "test_dag",
  dag_run_id: "run_1",
  dag_versions: [],
  end_date: null,
  has_missed_deadline: false,
  logical_date: null,
  note: null,
  partition_key: null,
  queued_at: null,
  run_after: "2025-01-01T00:00:00Z",
  run_type: "manual" as const,
  start_date: null,
  state,
  triggered_by: "ui" as const,
  triggering_user_name: null,
});

const openMenu = async (container: HTMLElement) => {
  const trigger = container.querySelector("[data-testid='mark-run-as-button']")!;

  await act(async () => {
    fireEvent.click(trigger);
  });
};

describe("MarkRunAsButton", () => {
  it("does not disable the 'success' option when the dag run is already in success state", async () => {
    const { container } = render(<MarkRunAsButton dagRun={makeDagRun("success")} />, {
      wrapper: Wrapper,
    });

    await openMenu(container);

    const successItem = await screen.findByTestId("mark-run-as-success");

    expect(successItem).not.toHaveAttribute("data-disabled");
    expect(successItem.closest("[disabled]")).toBeNull();
  });

  it("does not disable the 'failed' option when the dag run is already in failed state", async () => {
    const { container } = render(<MarkRunAsButton dagRun={makeDagRun("failed")} />, {
      wrapper: Wrapper,
    });

    await openMenu(container);

    const failedItem = await screen.findByTestId("mark-run-as-failed");

    expect(failedItem).not.toHaveAttribute("data-disabled");
    expect(failedItem.closest("[disabled]")).toBeNull();
  });

  it("opens the confirmation dialog when clicking 'success' on an already-succeeded dag run", async () => {
    const { container } = render(<MarkRunAsButton dagRun={makeDagRun("success")} />, {
      wrapper: Wrapper,
    });

    await openMenu(container);

    const successItem = await screen.findByTestId("mark-run-as-success");

    await act(async () => {
      fireEvent.click(successItem);
    });

    await waitFor(() => {
      expect(screen.getByTestId("mark-run-as-confirm")).toBeInTheDocument();
    });
  });
});
