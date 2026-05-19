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
import { beforeEach, describe, expect, it, vi } from "vitest";

import i18n from "src/i18n/config";
import { Wrapper } from "src/utils/Wrapper";

import dagLocale from "../../public/i18n/locales/en/dag.json";
import dashboardLocale from "../../public/i18n/locales/en/dashboard.json";
import { DagDeactivatedBanner } from "./DagDeactivatedBanner";

const { mockUseDagServiceGetDag, mockUseImportErrorServiceGetImportErrors } = vi.hoisted(() => ({
  mockUseDagServiceGetDag: vi.fn(),
  mockUseImportErrorServiceGetImportErrors: vi.fn(),
}));

vi.mock("react-router-dom", async () => {
  const actual = await vi.importActual("react-router-dom");

  return {
    ...actual,
    useParams: () => ({ dagId: "stale_dag" }),
  };
});

vi.mock("openapi/queries", async (importOriginal) => {
  // eslint-disable-next-line @typescript-eslint/consistent-type-imports -- `import()` type is the standard pattern for typing `importOriginal` in Vitest mocks.
  const actual = await importOriginal<typeof import("openapi/queries")>();

  return {
    ...actual,
    useDagServiceGetDag: mockUseDagServiceGetDag,
    useImportErrorServiceGetImportErrors: mockUseImportErrorServiceGetImportErrors,
  };
});

const staleDagQuery = {
  data: { is_stale: true, relative_fileloc: "stale_dag.py" },
  isLoading: false,
};

const notStaleDagQuery = {
  data: { is_stale: false, relative_fileloc: "stale_dag.py" },
  isLoading: false,
};

const emptyImportErrorsQuery = {
  data: { import_errors: [], total_entries: 0 },
  error: null,
  isError: false,
  isLoading: false,
  isPending: false,
};

describe("DagDeactivatedBanner", () => {
  beforeEach(() => {
    i18n.addResourceBundle("en", "dag", dagLocale, true, true);
    i18n.addResourceBundle("en", "dashboard", dashboardLocale, true, true);
    mockUseDagServiceGetDag.mockReturnValue(staleDagQuery);
    mockUseImportErrorServiceGetImportErrors.mockReturnValue(emptyImportErrorsQuery);
  });

  it("does not render when the dag is not stale", () => {
    mockUseDagServiceGetDag.mockReturnValue(notStaleDagQuery);

    render(
      <Wrapper>
        <DagDeactivatedBanner />
      </Wrapper>,
    );

    expect(screen.queryByText(i18n.t("header.status.deactivated", { ns: "dag" }))).not.toBeInTheDocument();
  });

  it("shows a deactivated banner when stale with no import error", () => {
    render(
      <Wrapper>
        <DagDeactivatedBanner />
      </Wrapper>,
    );

    expect(screen.getByText(i18n.t("header.status.deactivated", { ns: "dag" }))).toBeInTheDocument();
    expect(
      screen.queryByRole("button", {
        name: i18n.t("importErrors.dagImportError", { count: 1, ns: "dashboard" }),
      }),
    ).not.toBeInTheDocument();
  });

  it("shows a deactivated banner with an import error button when the API returns a file-scoped error", async () => {
    mockUseImportErrorServiceGetImportErrors.mockReturnValue({
      ...emptyImportErrorsQuery,
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
    });

    render(
      <Wrapper>
        <DagDeactivatedBanner />
      </Wrapper>,
    );

    expect(screen.getByText(i18n.t("header.status.deactivated", { ns: "dag" }))).toBeInTheDocument();

    const importErrorButton = screen.getByRole("button", {
      name: i18n.t("importErrors.dagImportError", { count: 1, ns: "dashboard" }),
    });

    expect(importErrorButton).toBeInTheDocument();
    expect(screen.queryByText(/SyntaxError: invalid syntax/u)).not.toBeInTheDocument();

    fireEvent.click(importErrorButton);

    await waitFor(() => {
      expect(screen.getByText("stale_dag.py")).toBeInTheDocument();
    });
    expect(screen.getByText(/SyntaxError: invalid syntax/u)).toBeInTheDocument();
  });
});
