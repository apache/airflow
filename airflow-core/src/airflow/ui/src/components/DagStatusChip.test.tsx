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
import { beforeEach, describe, expect, it, vi } from "vitest";

import i18n from "src/i18n/config";
import { Wrapper } from "src/utils/Wrapper";

import componentsLocale from "../../public/i18n/locales/en/components.json";
import dagLocale from "../../public/i18n/locales/en/dag.json";
import dashboardLocale from "../../public/i18n/locales/en/dashboard.json";
import { DagStatusChip } from "./DagStatusChip";

const {
  mockUseDagServiceGetDag,
  mockUseDagWarningServiceListDagWarnings,
  mockUseImportErrorServiceGetImportErrors,
} = vi.hoisted(() => ({
  mockUseDagServiceGetDag: vi.fn(),
  mockUseDagWarningServiceListDagWarnings: vi.fn(),
  mockUseImportErrorServiceGetImportErrors: vi.fn(),
}));

vi.mock("openapi/queries", async (importOriginal) => {
  // eslint-disable-next-line @typescript-eslint/consistent-type-imports -- `import()` type is the standard pattern for typing `importOriginal` in Vitest mocks.
  const actual = await importOriginal<typeof import("openapi/queries")>();

  return {
    ...actual,
    useDagServiceGetDag: mockUseDagServiceGetDag,
    useDagWarningServiceListDagWarnings: mockUseDagWarningServiceListDagWarnings,
    useImportErrorServiceGetImportErrors: mockUseImportErrorServiceGetImportErrors,
  };
});

const staleDagQuery = { data: { is_stale: true, relative_fileloc: "stale_dag.py" }, isLoading: false };
const liveDagQuery = { data: { is_stale: false, relative_fileloc: "stale_dag.py" }, isLoading: false };
const noImportErrors = { data: { import_errors: [], total_entries: 0 }, isLoading: false };
const noWarnings = { data: { dag_warnings: [], total_entries: 0 }, isLoading: false };

const importErrorQuery = {
  data: {
    import_errors: [
      {
        bundle_name: "dags-folder",
        filename: "stale_dag.py",
        import_error_id: 42,
        stack_trace: "Traceback (most recent call last):\nSyntaxError: invalid syntax",
        timestamp: "2025-02-01T12:00:00Z",
      },
    ],
    total_entries: 1,
  },
  isLoading: false,
};

const warningsQuery = {
  data: {
    dag_warnings: [{ message: "duplicate task id", warning_type: "non-existent pool" }],
    total_entries: 1,
  },
  isLoading: false,
};

const deactivated = () => i18n.t("header.status.deactivated", { ns: "dag" });
const importErrorName = () => i18n.t("importErrors.dagImportError", { count: 1, ns: "dashboard" });
const warningName = (count: number) => i18n.t("dagWarnings.warning", { count, ns: "components" });

const renderChip = () =>
  render(
    <Wrapper>
      <DagStatusChip dagId="stale_dag" />
    </Wrapper>,
  );

describe("DagStatusChip", () => {
  beforeEach(() => {
    i18n.addResourceBundle("en", "components", componentsLocale, true, true);
    i18n.addResourceBundle("en", "dag", dagLocale, true, true);
    i18n.addResourceBundle("en", "dashboard", dashboardLocale, true, true);
    mockUseDagServiceGetDag.mockReturnValue(staleDagQuery);
    mockUseImportErrorServiceGetImportErrors.mockReturnValue(noImportErrors);
    mockUseDagWarningServiceListDagWarnings.mockReturnValue(noWarnings);
  });

  it("renders nothing when the Dag is live and has no warnings", () => {
    mockUseDagServiceGetDag.mockReturnValue(liveDagQuery);

    renderChip();

    expect(screen.queryByRole("button")).not.toBeInTheDocument();
  });

  it("names the deactivation when that is the only status", () => {
    renderChip();

    expect(screen.getByRole("button", { name: deactivated() })).toBeInTheDocument();
  });

  it("names the warnings when the Dag is live", () => {
    mockUseDagServiceGetDag.mockReturnValue(liveDagQuery);
    mockUseDagWarningServiceListDagWarnings.mockReturnValue(warningsQuery);

    renderChip();

    expect(screen.getByRole("button", { name: warningName(1) })).toBeInTheDocument();
  });

  it("lets the parse error outrank the warnings for name and severity", async () => {
    mockUseImportErrorServiceGetImportErrors.mockReturnValue(importErrorQuery);
    mockUseDagWarningServiceListDagWarnings.mockReturnValue(warningsQuery);

    renderChip();

    const chip = screen.getByRole("button", { name: importErrorName() });

    fireEvent.click(chip);

    // Both land in the one modal, the parse error ahead of the warning.
    await waitFor(() => expect(screen.getByText(/SyntaxError: invalid syntax/u)).toBeInTheDocument());
    expect(screen.getByText("duplicate task id")).toBeInTheDocument();
    // The parse error is why the Dag went stale, so the list does not state the staleness again.
    expect(screen.queryByText(deactivated())).not.toBeInTheDocument();
  });

  it("reports a page load failure as an error beside the warnings", async () => {
    mockUseDagServiceGetDag.mockReturnValue(liveDagQuery);
    mockUseDagWarningServiceListDagWarnings.mockReturnValue(warningsQuery);

    render(
      <Wrapper>
        <DagStatusChip dagId="stale_dag" error={new Error("request failed")} />
      </Wrapper>,
    );

    fireEvent.click(
      screen.getByRole("button", { name: i18n.t("dagWarnings.error_one", { ns: "components" }) }),
    );

    // The failure itself has to reach the list, not just the warnings it is listed beside.
    await waitFor(() => expect(screen.getByText("request failed")).toBeInTheDocument());
    expect(screen.getByText("duplicate task id")).toBeInTheDocument();
  });
});
