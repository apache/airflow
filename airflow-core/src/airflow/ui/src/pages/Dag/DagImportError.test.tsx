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

import dashboardLocale from "../../../public/i18n/locales/en/dashboard.json";
import { DagImportError } from "./DagImportError";

const { mockUseImportErrorServiceGetImportErrors } = vi.hoisted(() => ({
  mockUseImportErrorServiceGetImportErrors: vi.fn(),
}));

vi.mock("openapi/queries", async (importOriginal) => {
  // eslint-disable-next-line @typescript-eslint/consistent-type-imports -- `import()` type is the standard pattern for typing `importOriginal` in Vitest mocks.
  const actual = await importOriginal<typeof import("openapi/queries")>();

  return {
    ...actual,
    useImportErrorServiceGetImportErrors: mockUseImportErrorServiceGetImportErrors,
  };
});

const emptyImportErrorsQuery = {
  data: { import_errors: [], total_entries: 0 },
  error: null,
  isError: false,
  isLoading: false,
  isPending: false,
};

const staleDagFields = {
  bundle_name: "dags-folder",
  is_stale: true,
  relative_fileloc: "stale_dag.py",
} as const;

describe("DagImportError", () => {
  beforeEach(() => {
    i18n.addResourceBundle("en", "dashboard", dashboardLocale, true, true);
    mockUseImportErrorServiceGetImportErrors.mockReturnValue(emptyImportErrorsQuery);
  });

  it("does not render when there is no matching import error", () => {
    render(
      <Wrapper>
        <DagImportError dag={staleDagFields} />
      </Wrapper>,
    );

    expect(screen.queryByTestId("dag-import-error")).not.toBeInTheDocument();
  });

  it("shows a matching import error when the API returns a file-scoped error", async () => {
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
        <DagImportError dag={staleDagFields} />
      </Wrapper>,
    );

    expect(screen.getByTestId("dag-import-error")).toBeInTheDocument();
    const openButton = screen.getByRole("button", {
      name: i18n.t("importErrors.dagImportError", { count: 1, ns: "dashboard" }),
    });

    expect(openButton).toBeInTheDocument();
    expect(screen.queryByText(/SyntaxError: invalid syntax/u)).not.toBeInTheDocument();

    fireEvent.click(openButton);

    await waitFor(() => {
      expect(
        screen.getByText(i18n.t("importErrors.dagImportError", { count: 1, ns: "dashboard" })),
      ).toBeInTheDocument();
    });
    expect(screen.getByText("stale_dag.py")).toBeInTheDocument();
    expect(screen.getByText(/SyntaxError: invalid syntax/u)).toBeInTheDocument();
  });

  it("does not render when the dag is not stale", () => {
    mockUseImportErrorServiceGetImportErrors.mockReturnValue({
      ...emptyImportErrorsQuery,
      data: {
        import_errors: [
          {
            bundle_name: "dags-folder",
            filename: "stale_dag.py",
            import_error_id: 1,
            stack_trace: "would match if stale",
            timestamp: "2025-02-01T12:00:00Z",
          },
        ],
        total_entries: 1,
      },
    });

    render(
      <Wrapper>
        <DagImportError dag={{ ...staleDagFields, is_stale: false }} />
      </Wrapper>,
    );

    expect(screen.queryByTestId("dag-import-error")).not.toBeInTheDocument();
  });

  it("does not render when bundle_name does not match", () => {
    mockUseImportErrorServiceGetImportErrors.mockReturnValue({
      ...emptyImportErrorsQuery,
      data: {
        import_errors: [
          {
            bundle_name: "other-bundle",
            filename: "stale_dag.py",
            import_error_id: 1,
            stack_trace: "wrong bundle",
            timestamp: "2025-02-01T12:00:00Z",
          },
        ],
        total_entries: 1,
      },
    });

    render(
      <Wrapper>
        <DagImportError dag={staleDagFields} />
      </Wrapper>,
    );

    expect(screen.queryByTestId("dag-import-error")).not.toBeInTheDocument();
  });
});
