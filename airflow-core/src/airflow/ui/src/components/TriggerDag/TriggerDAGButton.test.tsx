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

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    i18n: { language: "en" },
    // eslint-disable-next-line id-length
    t: (translationKey: string) =>
      ({
        "triggerDag.button": "Trigger",
        "triggerDag.manualRunDenied": "Manual runs are not allowed for this Dag",
        "triggerDag.title": "Trigger Dag",
        "triggerDag.triggerAgainWithConfig": "Trigger again with this config",
        "triggerDag.triggerOptions": "Trigger options",
        "triggerDag.triggerWithConfig": "Trigger with config",
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

afterEach(() => {
  cleanup();
  routeParams.runId = "";
});

beforeEach(() => {
  useDagRunServiceGetDagRunMock.mockReset();
  useDagRunServiceGetDagRunMock.mockReturnValue({ data: undefined, isLoading: false });
});

describe("TriggerDAGButton", () => {
  it("opens the trigger form from the main button without opening the dropdown", () => {
    render(<TriggerDAGButton dagDisplayName="test-dag" dagId="test_dag" isPaused={false} />, { wrapper });

    fireEvent.click(screen.getByTestId("trigger-dag-button"));

    expect(screen.getByTestId("trigger-modal")).toBeInTheDocument();
    expect(screen.queryByText("Trigger with config")).not.toBeInTheDocument();
  });

  it("opens a menu from the caret with trigger options", async () => {
    render(<TriggerDAGButton dagDisplayName="test-dag" dagId="test_dag" isPaused={false} />, { wrapper });

    fireEvent.click(screen.getByTestId("trigger-dag-options-button"));

    await waitFor(() => {
      expect(screen.getByRole("menuitem", { name: "Trigger with config" })).toBeInTheDocument();
    });
  });

  it("opens the trigger form from the Trigger with config menu item", async () => {
    render(<TriggerDAGButton dagDisplayName="test-dag" dagId="test_dag" isPaused={false} />, { wrapper });

    fireEvent.click(screen.getByTestId("trigger-dag-options-button"));
    fireEvent.click(await screen.findByRole("menuitem", { name: "Trigger with config" }));

    expect(screen.getByTestId("trigger-modal")).toBeInTheDocument();
  });

  it("offers Trigger again with this config when a selected Dag run has config", async () => {
    routeParams.runId = "manual__run_1";
    useDagRunServiceGetDagRunMock.mockReturnValue({
      data: {
        conf: { foo: "bar" },
        dag_run_id: "manual__run_1",
        logical_date: "2024-01-01T00:00:00Z",
      },
      isLoading: false,
    });

    render(<TriggerDAGButton dagDisplayName="test-dag" dagId="test_dag" isPaused={false} />, { wrapper });

    fireEvent.click(screen.getByTestId("trigger-dag-options-button"));

    await waitFor(() => {
      expect(screen.getByRole("menuitem", { name: "Trigger again with this config" })).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("menuitem", { name: "Trigger again with this config" }));

    expect(screen.getByTestId("trigger-modal")).toHaveAttribute("data-prefilled", "true");
  });

  it("does not offer Trigger again with this config without a selected Dag run", async () => {
    render(<TriggerDAGButton dagDisplayName="test-dag" dagId="test_dag" isPaused={false} />, { wrapper });

    fireEvent.click(screen.getByTestId("trigger-dag-options-button"));

    const menuItems = await screen.findAllByRole("menuitem");

    expect(menuItems).toHaveLength(1);
    expect(
      screen.queryByRole("menuitem", { name: "Trigger again with this config" }),
    ).not.toBeInTheDocument();
  });

  it("opens the menu and activates an item with the keyboard", async () => {
    render(<TriggerDAGButton dagDisplayName="test-dag" dagId="test_dag" isPaused={false} />, { wrapper });

    const optionsButton = screen.getByTestId("trigger-dag-options-button");

    optionsButton.focus();
    fireEvent.keyDown(optionsButton, { key: "ArrowDown" });
    const menuItem = await screen.findByRole("menuitem", { name: "Trigger with config" });

    fireEvent.keyDown(menuItem, { key: "Enter" });

    await screen.findByTestId("trigger-modal");
  });

  it("disables both the main button and the caret when manual runs are not allowed", () => {
    render(
      <TriggerDAGButton
        allowedRunTypes={["scheduled"]}
        dagDisplayName="test-dag"
        dagId="test_dag"
        isPaused={false}
      />,
      { wrapper },
    );

    expect(screen.getByTestId("trigger-dag-button")).toBeDisabled();
    expect(screen.getByTestId("trigger-dag-options-button")).toBeDisabled();
  });
});
