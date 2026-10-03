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
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
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
      })[translationKey] ?? translationKey,
  }),
}));

vi.mock("./TriggerDAGModal", () => ({
  default: ({ open }: { readonly open: boolean }) =>
    open ? <div data-testid="trigger-modal">Trigger Modal</div> : null,
}));

const props = { dagDisplayName: "My Dag", dagId: "my_dag", isPaused: false };

afterEach(() => {
  cleanup();
  routeParams.runId = "";
});

beforeEach(() => {
  useDagRunServiceGetDagRunMock.mockReset();
  useDagRunServiceGetDagRunMock.mockReturnValue({ data: undefined });
});

describe("TriggerDAGButton", () => {
  it("opens the form directly, with no config menu, when the selected run has an empty config", () => {
    routeParams.runId = "run_empty_conf";
    useDagRunServiceGetDagRunMock.mockReturnValue({
      data: { conf: {}, dag_run_id: "run_empty_conf", logical_date: "2026-01-01T00:00:00Z" },
    });

    render(<TriggerDAGButton {...props} withText />, { wrapper });

    fireEvent.click(screen.getByTestId("trigger-dag-button"));

    expect(screen.getByTestId("trigger-modal")).toBeInTheDocument();
    expect(screen.queryByText("Trigger again with this config")).not.toBeInTheDocument();
  });

  it("shows the config menu when the selected run has a non-empty config", async () => {
    routeParams.runId = "run_with_conf";
    useDagRunServiceGetDagRunMock.mockReturnValue({
      data: { conf: { country: "FR" }, dag_run_id: "run_with_conf", logical_date: "2026-01-01T00:00:00Z" },
    });

    render(<TriggerDAGButton {...props} withText />, { wrapper });

    fireEvent.click(screen.getByTestId("trigger-dag-button"));

    expect(await screen.findByText("Trigger again with this config")).toBeInTheDocument();
    expect(screen.queryByTestId("trigger-modal")).not.toBeInTheDocument();
  });
});
