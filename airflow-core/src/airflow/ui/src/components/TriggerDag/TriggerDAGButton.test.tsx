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
import type { PropsWithChildren } from "react";

import "@testing-library/jest-dom";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import { TriggerDAGButton } from "./TriggerDAGButton";

const wrapper = ({ children }: PropsWithChildren) => (
  <BaseWrapper>
    <MemoryRouter>{children}</MemoryRouter>
  </BaseWrapper>
);

const routeParams: Record<string, string> = {};

vi.mock("react-router-dom", async () => {
  const actual = await vi.importActual("react-router-dom");

  return {
    ...actual,
    useParams: () => routeParams,
  };
});

const useDagRunServiceGetDagRunMock = vi.hoisted(() => vi.fn());

vi.mock("openapi/queries", () => ({
  useDagRunServiceGetDagRun: useDagRunServiceGetDagRunMock,
}));

const triggerDagRunMock = vi.hoisted(() => vi.fn());

vi.mock("src/queries/useTrigger", () => ({
  useTrigger: () => ({ error: undefined, isPending: false, triggerDagRun: triggerDagRunMock }),
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    i18n: { language: "en" },
    // eslint-disable-next-line id-length
    t: (translationKey: string) =>
      ({
        "triggerDag.button": "Trigger",
        "triggerDag.editConfigAndTrigger": "Edit config and trigger",
        "triggerDag.manualRunDenied": "Manual runs are not allowed for this Dag",
        "triggerDag.title": "Trigger Dag",
        "triggerDag.triggerAgainWithConfig": "Trigger again with this config",
        "triggerDag.triggerOptions": "Trigger options",
      })[translationKey] ?? translationKey,
  }),
}));

vi.mock("./TriggerDAGModal", () => ({
  default: ({ open, prefillConfig }: { readonly open: boolean; readonly prefillConfig: unknown }) =>
    open ? (
      <div data-prefilled={prefillConfig !== undefined} data-testid="trigger-modal">
        Trigger Modal
      </div>
    ) : null,
}));

const props = { dagDisplayName: "My Dag", dagId: "my_dag", isPaused: false };

afterEach(() => {
  cleanup();
  routeParams.runId = "";
});

beforeEach(() => {
  useDagRunServiceGetDagRunMock.mockReset();
  useDagRunServiceGetDagRunMock.mockReturnValue({ data: undefined });
  triggerDagRunMock.mockReset();
});

describe("TriggerDAGButton", () => {
  it("has no options caret when no run with a config is selected", () => {
    render(<TriggerDAGButton {...props} />, { wrapper });

    expect(screen.getByTestId("trigger-dag-button")).toBeInTheDocument();
    expect(screen.queryByTestId("trigger-dag-options-button")).not.toBeInTheDocument();
  });

  it("opens the trigger form in one click, without a prefilled config", () => {
    render(<TriggerDAGButton {...props} />, { wrapper });

    fireEvent.click(screen.getByTestId("trigger-dag-button"));

    const modal = screen.getByTestId("trigger-modal");

    expect(modal).toBeInTheDocument();
    expect(modal).toHaveAttribute("data-prefilled", "false");
  });

  it("treats an empty config as no config: one click, no options caret", () => {
    routeParams.runId = "run_empty_conf";
    useDagRunServiceGetDagRunMock.mockReturnValue({
      data: { conf: {}, dag_run_id: "run_empty_conf", logical_date: "2026-01-01T00:00:00Z" },
    });

    render(<TriggerDAGButton {...props} />, { wrapper });

    expect(screen.queryByTestId("trigger-dag-options-button")).not.toBeInTheDocument();
    fireEvent.click(screen.getByTestId("trigger-dag-button"));
    expect(screen.getByTestId("trigger-modal")).toHaveAttribute("data-prefilled", "false");
  });

  it("shows the options caret when the selected run carried a config", () => {
    routeParams.runId = "run_with_conf";
    useDagRunServiceGetDagRunMock.mockReturnValue({
      data: { conf: { country: "FR" }, dag_run_id: "run_with_conf", logical_date: "2026-01-01T00:00:00Z" },
    });

    render(<TriggerDAGButton {...props} />, { wrapper });

    expect(screen.getByTestId("trigger-dag-options-button")).toBeInTheDocument();
    // The main button still triggers in one click with no prefill, rather than opening the menu.
    fireEvent.click(screen.getByTestId("trigger-dag-button"));
    expect(screen.getByTestId("trigger-modal")).toHaveAttribute("data-prefilled", "false");
  });

  it("re-triggers directly with the selected run's config, bypassing the form", async () => {
    routeParams.runId = "run_with_conf";
    useDagRunServiceGetDagRunMock.mockReturnValue({
      data: { conf: { country: "FR" }, dag_run_id: "run_with_conf", logical_date: "2026-01-01T00:00:00Z" },
    });

    render(<TriggerDAGButton {...props} />, { wrapper });

    fireEvent.click(screen.getByTestId("trigger-dag-options-button"));
    fireEvent.click(await screen.findByText("Trigger again with this config"));

    await waitFor(() =>
      expect(triggerDagRunMock).toHaveBeenCalledWith(
        expect.objectContaining({ conf: JSON.stringify({ country: "FR" }) }),
      ),
    );
    expect(screen.queryByTestId("trigger-modal")).not.toBeInTheDocument();
  });

  it("opens the prefilled form from the edit-config option", async () => {
    routeParams.runId = "run_with_conf";
    useDagRunServiceGetDagRunMock.mockReturnValue({
      data: { conf: { country: "FR" }, dag_run_id: "run_with_conf", logical_date: "2026-01-01T00:00:00Z" },
    });

    render(<TriggerDAGButton {...props} />, { wrapper });

    fireEvent.click(screen.getByTestId("trigger-dag-options-button"));
    fireEvent.click(await screen.findByText("Edit config and trigger"));

    await waitFor(() =>
      expect(screen.getByTestId("trigger-modal")).toHaveAttribute("data-prefilled", "true"),
    );
    expect(triggerDagRunMock).not.toHaveBeenCalled();
  });

  it("disables both the trigger and the options caret when manual runs are denied", () => {
    routeParams.runId = "run_with_conf";
    useDagRunServiceGetDagRunMock.mockReturnValue({
      data: { conf: { country: "FR" }, dag_run_id: "run_with_conf", logical_date: "2026-01-01T00:00:00Z" },
    });

    render(<TriggerDAGButton {...props} allowedRunTypes={["backfill"]} withText />, { wrapper });

    expect(screen.getByTestId("trigger-dag-button")).toBeDisabled();
    expect(screen.getByTestId("trigger-dag-options-button")).toBeDisabled();
  });
});
