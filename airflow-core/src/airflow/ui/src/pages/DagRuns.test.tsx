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
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { setupServer, type SetupServerApi } from "msw/node";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";

import { handlers } from "src/mocks/handlers";
import { AppWrapper } from "src/utils/AppWrapper";

let server: SetupServerApi;

beforeAll(() => {
  server = setupServer(...handlers);
  server.listen({ onUnhandledRequest: "bypass" });
});

afterEach(() => server.resetHandlers());
afterAll(() => server.close());

// The dag_runs mock handler (see src/mocks/handlers/dag_runs.ts) returns:
//   - run_before_filter (logical_date: 2024-12-31) — excluded when filtering Jan 2025
//   - run_in_range      (logical_date: 2025-01-15) — included when filtering Jan 2025
describe("DagRuns logical date filter", () => {
  it("shows all runs when no logical date filter is applied", async () => {
    render(<AppWrapper initialEntries={["/dag_runs"]} />);

    await waitFor(() => expect(screen.getByText("run_in_range")).toBeInTheDocument());
    expect(screen.getByText("run_before_filter")).toBeInTheDocument();
  });

  it("filters runs by logical_date_gte and logical_date_lte URL params", async () => {
    render(
      <AppWrapper
        initialEntries={[
          "/dag_runs?logical_date_gte=2025-01-01T00%3A00%3A00Z&logical_date_lte=2025-01-31T23%3A59%3A59Z",
        ]}
      />,
    );

    await waitFor(() => expect(screen.getByText("run_in_range")).toBeInTheDocument());
    expect(screen.queryByText("run_before_filter")).not.toBeInTheDocument();
  });
});

describe("DagRuns row selection", () => {
  it("renders a select checkbox per row and reveals the action bar on selection", async () => {
    render(<AppWrapper initialEntries={["/dag_runs"]} />);

    await waitFor(() => expect(screen.getByText("run_in_range")).toBeInTheDocument());

    // Each data row carries a leading checkbox; clicking it should expose the
    // bulk-action ActionBar (signalled by the "1 Selected" counter).
    const runRow = screen.getByText("run_in_range").closest("tr");

    expect(runRow).not.toBeNull();
    const [rowCheckbox] = within(runRow as HTMLElement).getAllByRole("checkbox");

    expect(rowCheckbox).toBeDefined();
    fireEvent.click(rowCheckbox as HTMLElement);

    await waitFor(() => expect(screen.getByText(/1\s+Selected/iu)).toBeInTheDocument());
  });
});

const selectRow = async (runText: string) => {
  await waitFor(() => expect(screen.getByText(runText)).toBeInTheDocument());
  const row = screen.getByText(runText).closest("tr");

  if (row === null) {
    throw new Error(`Row for ${runText} not found`);
  }
  const [checkbox] = within(row).getAllByRole("checkbox");

  if (checkbox === undefined) {
    throw new Error(`Checkbox in row for ${runText} not found`);
  }
  fireEvent.click(checkbox);

  return row;
};

// Per-row buttons are Chakra IconButtons whose accessible name comes from
// `aria-label` and whose textContent only contains icon glyphs / single-char
// separators. The bulk-action buttons in the ActionBar render the translated
// label as a visible text node. We pick the button whose textContent itself
// matches the label regex, which is independent of locale state (the regex
// matches the i18n key under tests, the translated string in production).
const findBulkActionButton = (label: RegExp) =>
  screen.getAllByRole("button", { name: label }).find((btn) => label.test(btn.textContent));

describe("DagRuns bulk delete", () => {
  it("fires one DELETE per selected run and closes the dialog on success", async () => {
    const onDelete = vi.fn();

    server.use(
      http.delete("/api/v2/dags/:dagId/dagRuns/:dagRunId", ({ params }) => {
        onDelete(params);

        return new HttpResponse(null, { status: 204 });
      }),
    );

    render(<AppWrapper initialEntries={["/dag_runs"]} />);

    await selectRow("run_in_range");
    await selectRow("run_before_filter");

    await waitFor(() => expect(screen.getByText(/2\s+Selected/iu)).toBeInTheDocument());

    const bulkDeleteBtn = findBulkActionButton(/delete/iu);

    expect(bulkDeleteBtn).toBeDefined();
    fireEvent.click(bulkDeleteBtn as HTMLElement);

    // Chakra's ActionBar.Root itself has role="dialog", so wait for the
    // confirm dialog (the second one) to mount and pick it explicitly.
    await waitFor(() => expect(screen.getAllByRole("dialog")).toHaveLength(2));
    const dialogs = screen.getAllByRole("dialog");
    const confirmDialog = dialogs[dialogs.length - 1] as HTMLElement;

    // The dialog renders each selected run as `<dag_id> / <run_id>`; match on
    // the run-id substring (regex matchers do partial matching on normalized
    // text content, regardless of element splits).
    expect(within(confirmDialog).getByText(/run_in_range/u)).toBeInTheDocument();
    expect(within(confirmDialog).getByText(/run_before_filter/u)).toBeInTheDocument();

    fireEvent.click(within(confirmDialog).getByRole("button", { name: /confirm/iu }));

    await waitFor(() => expect(onDelete).toHaveBeenCalledTimes(2));
    expect(onDelete).toHaveBeenCalledWith(
      expect.objectContaining({ dagId: "test_dag", dagRunId: "run_in_range" }),
    );
    expect(onDelete).toHaveBeenCalledWith(
      expect.objectContaining({ dagId: "test_dag", dagRunId: "run_before_filter" }),
    );

    // Dialog should auto-close once both deletions resolve successfully.
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
  });

  it("keeps the dialog open and surfaces an error on partial failure", async () => {
    server.use(
      http.delete("/api/v2/dags/:dagId/dagRuns/:dagRunId", ({ params }) =>
        params.dagRunId === "run_in_range"
          ? new HttpResponse(null, { status: 204 })
          : HttpResponse.json({ detail: "boom" }, { status: 500 }),
      ),
    );

    render(<AppWrapper initialEntries={["/dag_runs"]} />);

    await selectRow("run_in_range");
    await selectRow("run_before_filter");

    await waitFor(() => expect(screen.getByText(/2\s+Selected/iu)).toBeInTheDocument());
    const bulkDeleteBtn = findBulkActionButton(/delete/iu);

    expect(bulkDeleteBtn).toBeDefined();
    fireEvent.click(bulkDeleteBtn as HTMLElement);

    await waitFor(() => expect(screen.getAllByRole("dialog")).toHaveLength(2));
    const dialogs = screen.getAllByRole("dialog");
    const confirmDialog = dialogs[dialogs.length - 1] as HTMLElement;

    fireEvent.click(within(confirmDialog).getByRole("button", { name: /confirm/iu }));

    // Confirm dialog stays open and shows the rejection — no auto-close
    // because one request failed; the partial success is still reported via
    // the toaster (not asserted here, since the Toaster portal lives outside
    // AppWrapper).
    await waitFor(() => expect(within(confirmDialog).getByText(/boom/iu)).toBeInTheDocument());
    expect(confirmDialog).toBeInTheDocument();
  });
});

describe("DagRuns bulk clear", () => {
  it("fires one /clear per selected run with the dialog options", async () => {
    const onClear = vi.fn<(params: Record<string, string>, body: unknown) => void>();

    server.use(
      http.post("/api/v2/dags/:dagId/dagRuns/:dagRunId/clear", async ({ params, request }) => {
        const body: unknown = await request.json();

        onClear(params as Record<string, string>, body);

        return HttpResponse.json({ dag_id: params.dagId, dag_run_id: params.dagRunId });
      }),
    );

    render(<AppWrapper initialEntries={["/dag_runs"]} />);

    await selectRow("run_in_range");
    await selectRow("run_before_filter");
    await waitFor(() => expect(screen.getByText(/2\s+Selected/iu)).toBeInTheDocument());

    const bulkClearBtn = findBulkActionButton(/clear/iu);

    expect(bulkClearBtn).toBeDefined();
    fireEvent.click(bulkClearBtn as HTMLElement);

    await waitFor(() => expect(screen.getAllByRole("dialog")).toHaveLength(2));
    const dialogs = screen.getAllByRole("dialog");
    const confirmDialog = dialogs[dialogs.length - 1] as HTMLElement;

    fireEvent.click(within(confirmDialog).getByRole("button", { name: /confirm/iu }));

    await waitFor(() => expect(onClear).toHaveBeenCalledTimes(2));
    expect(onClear).toHaveBeenCalledWith(
      expect.objectContaining({ dagId: "test_dag", dagRunId: "run_in_range" }),
      expect.objectContaining({ dry_run: false, only_failed: false, only_new: false }),
    );
    expect(onClear).toHaveBeenCalledWith(
      expect.objectContaining({ dagId: "test_dag", dagRunId: "run_before_filter" }),
      expect.objectContaining({ dry_run: false, only_failed: false, only_new: false }),
    );
  });
});

describe("DagRuns bulk mark-as", () => {
  it("fires one PATCH with the chosen state per affected run", async () => {
    const onPatch = vi.fn<(params: Record<string, string>, body: unknown) => void>();

    server.use(
      http.patch("/api/v2/dags/:dagId/dagRuns/:dagRunId", async ({ params, request }) => {
        const body: unknown = await request.json();

        onPatch(params as Record<string, string>, body);

        return HttpResponse.json({ dag_id: params.dagId, dag_run_id: params.dagRunId });
      }),
    );

    render(<AppWrapper initialEntries={["/dag_runs"]} />);

    await selectRow("run_in_range");
    await selectRow("run_before_filter");
    await waitFor(() => expect(screen.getByText(/2\s+Selected/iu)).toBeInTheDocument());

    // Open the Mark As menu — both seeded runs are in "success" state, so
    // only the "failed" entry has a non-zero affected count.
    const bulkMarkBtn = findBulkActionButton(/mark/iu);

    expect(bulkMarkBtn).toBeDefined();
    fireEvent.click(bulkMarkBtn as HTMLElement);

    // In the bulk Mark-As menu, items are plain Menu.Item nodes (no
    // `data-testid`); the per-row MarkRunAsButton menus render their own
    // menuitems with `data-testid="mark-run-as-*"`. Pick by absence of that
    // testid so we always target the bulk menu regardless of which menu is
    // open.
    await waitFor(() => expect(screen.getAllByRole("menuitem").length).toBeGreaterThan(0));
    const failedItem = screen
      .getAllByRole("menuitem")
      .find((mi) => mi.dataset.value === "failed" && mi.dataset.testid === undefined);

    expect(failedItem).toBeDefined();
    fireEvent.click(failedItem as HTMLElement);

    await waitFor(() => expect(screen.getAllByRole("dialog")).toHaveLength(2));
    const dialogs = screen.getAllByRole("dialog");
    const confirmDialog = dialogs[dialogs.length - 1] as HTMLElement;

    fireEvent.click(within(confirmDialog).getByRole("button", { name: /confirm/iu }));

    await waitFor(() => expect(onPatch).toHaveBeenCalledTimes(2));
    expect(onPatch).toHaveBeenCalledWith(
      expect.objectContaining({ dagId: "test_dag", dagRunId: "run_in_range" }),
      expect.objectContaining({ state: "failed" }),
    );
  });
});
