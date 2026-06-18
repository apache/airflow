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
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { PropsWithChildren } from "react";
import { MemoryRouter, useLocation } from "react-router-dom";
import { afterEach, describe, expect, it } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import { SavedViewsMenu } from "./SavedViewsMenu";

const LocationProbe = () => {
  const location = useLocation();

  return <div data-testid="location-search">{location.search}</div>;
};

const createWrapper =
  (initialEntries: Array<string> = ["/dags"]) =>
  ({ children }: PropsWithChildren) => (
    <BaseWrapper>
      <MemoryRouter initialEntries={initialEntries}>
        {children}
        <LocationProbe />
      </MemoryRouter>
    </BaseWrapper>
  );

const openMenu = async () => {
  fireEvent.click(screen.getByTestId("saved-views-button"));

  return screen.findByTestId("saved-view-name");
};

afterEach(() => {
  cleanup();
  localStorage.clear();
});

describe("SavedViewsMenu", () => {
  it("shows an empty state when no views are saved", async () => {
    render(<SavedViewsMenu />, { wrapper: createWrapper() });
    await openMenu();

    expect(screen.getByText("savedViews.empty")).toBeInTheDocument();
  });

  it("saves the current view, dropping pagination and baking in the active sort", async () => {
    localStorage.setItem("dags-table-sort", JSON.stringify([{ desc: true, id: "start_date" }]));

    render(<SavedViewsMenu />, { wrapper: createWrapper(["/dags?state=running&offset=40"]) });
    const input = await openMenu();

    fireEvent.change(input, { target: { value: "Running runs" } });
    fireEvent.click(screen.getByTestId("saved-view-save"));

    await waitFor(() => {
      expect(JSON.parse(localStorage.getItem("dags-saved-views") ?? "[]")).toEqual([
        { name: "Running runs", search: "state=running&sort=-start_date" },
      ]);
    });
    // The input is reset to its placeholder so it is clear the view was saved.
    expect(input).toHaveValue("");
  });

  it("does not save a view with a blank name", async () => {
    render(<SavedViewsMenu />, { wrapper: createWrapper(["/dags?state=running"]) });
    const input = await openMenu();

    fireEvent.change(input, { target: { value: "   " } });

    expect(screen.getByTestId("saved-view-save")).toBeDisabled();
  });

  it("disables save on the default view, even with a stored sort and a typed name", async () => {
    // A bare table page keeps a default sort in localStorage but no URL filters — still nothing to save.
    localStorage.setItem("dags-table-sort", JSON.stringify([{ desc: true, id: "run_after" }]));

    render(<SavedViewsMenu />, { wrapper: createWrapper(["/dags"]) });
    const input = await openMenu();

    fireEvent.change(input, { target: { value: "My view" } });

    expect(screen.getByTestId("saved-view-save")).toBeDisabled();
  });

  it("overwrites an existing view with the same name", async () => {
    localStorage.setItem(
      "dags-saved-views",
      JSON.stringify([{ name: "Running runs", search: "state=queued" }]),
    );

    render(<SavedViewsMenu />, { wrapper: createWrapper(["/dags?state=running"]) });
    const input = await openMenu();

    fireEvent.change(input, { target: { value: "Running runs" } });
    fireEvent.click(screen.getByTestId("saved-view-save"));

    await waitFor(() => {
      expect(JSON.parse(localStorage.getItem("dags-saved-views") ?? "[]")).toEqual([
        { name: "Running runs", search: "state=running" },
      ]);
    });
  });

  it("restores a saved view to the URL and the table sort", async () => {
    localStorage.setItem(
      "dags-saved-views",
      JSON.stringify([{ name: "Successful runs", search: "state=success&sort=-run_after" }]),
    );

    render(<SavedViewsMenu />, { wrapper: createWrapper(["/dags"]) });
    await openMenu();

    fireEvent.click(screen.getByText("Successful runs"));

    // The sort is restored to localStorage (where the table reads it), not left in the URL.
    await waitFor(() => {
      expect(screen.getByTestId("location-search")).toHaveTextContent("state=success");
    });
    expect(screen.getByTestId("location-search")).not.toHaveTextContent("sort");
    expect(JSON.parse(localStorage.getItem("dags-table-sort") ?? "[]")).toEqual([
      { desc: true, id: "run_after" },
    ]);
  });

  it("deletes a saved view after confirming", async () => {
    localStorage.setItem(
      "dags-saved-views",
      JSON.stringify([
        { name: "Successful runs", search: "state=success" },
        { name: "Failed runs", search: "state=failed" },
      ]),
    );

    render(<SavedViewsMenu />, { wrapper: createWrapper(["/dags"]) });
    await openMenu();

    fireEvent.click(screen.getAllByLabelText("Delete view")[0] as HTMLElement);
    fireEvent.click(await screen.findByTestId("delete-confirm-button"));

    await waitFor(() => {
      expect(JSON.parse(localStorage.getItem("dags-saved-views") ?? "[]")).toEqual([
        { name: "Failed runs", search: "state=failed" },
      ]);
    });
  });

  it("keeps the saved view when the delete is cancelled", async () => {
    localStorage.setItem(
      "dags-saved-views",
      JSON.stringify([{ name: "Successful runs", search: "state=success" }]),
    );

    render(<SavedViewsMenu />, { wrapper: createWrapper(["/dags"]) });
    await openMenu();

    fireEvent.click(screen.getByLabelText("Delete view"));
    fireEvent.click(await screen.findByTestId("delete-cancel-button"));

    await waitFor(() => {
      expect(screen.queryByTestId("delete-confirm-button")).not.toBeInTheDocument();
    });
    expect(JSON.parse(localStorage.getItem("dags-saved-views") ?? "[]")).toEqual([
      { name: "Successful runs", search: "state=success" },
    ]);
  });

  it("blocks save when the exact setup is already saved under another name", async () => {
    localStorage.setItem(
      "dags-saved-views",
      JSON.stringify([{ name: "Running runs", search: "state=running" }]),
    );

    render(<SavedViewsMenu />, { wrapper: createWrapper(["/dags?state=running"]) });
    const input = await openMenu();

    fireEvent.change(input, { target: { value: "Another name" } });

    expect(screen.getByTestId("saved-view-save")).toBeDisabled();
  });

  it("restores the default view automatically on a bare page load", async () => {
    localStorage.setItem(
      "dags-saved-views",
      JSON.stringify([{ name: "Successful runs", search: "state=success&sort=-run_after" }]),
    );
    localStorage.setItem("dags-saved-views-default", JSON.stringify("Successful runs"));

    render(<SavedViewsMenu />, { wrapper: createWrapper(["/dags"]) });

    // The default view is restored with its filters in the URL and its sort in localStorage only.
    await waitFor(() => {
      expect(screen.getByTestId("location-search")).toHaveTextContent("state=success");
    });
    expect(screen.getByTestId("location-search")).not.toHaveTextContent("sort");
    expect(JSON.parse(localStorage.getItem("dags-table-sort") ?? "[]")).toEqual([
      { desc: true, id: "run_after" },
    ]);
  });

  it("restores the default view even when only a leftover sort is in the URL", async () => {
    localStorage.setItem(
      "dags-saved-views",
      JSON.stringify([{ name: "Successful runs", search: "state=success" }]),
    );
    localStorage.setItem("dags-saved-views-default", JSON.stringify("Successful runs"));

    render(<SavedViewsMenu />, { wrapper: createWrapper(["/dags?sort=-run_after"]) });

    await waitFor(() => {
      expect(screen.getByTestId("location-search")).toHaveTextContent("state=success");
    });
  });

  it("does not restore the default view when the page already has filters", async () => {
    localStorage.setItem(
      "dags-saved-views",
      JSON.stringify([{ name: "Successful runs", search: "state=success" }]),
    );
    localStorage.setItem("dags-saved-views-default", JSON.stringify("Successful runs"));

    render(<SavedViewsMenu />, { wrapper: createWrapper(["/dags?state=running"]) });
    await openMenu();

    expect(screen.getByTestId("location-search")).toHaveTextContent("state=running");
    expect(screen.getByTestId("location-search")).not.toHaveTextContent("state=success");
  });

  it("toggles a view as the default and back", async () => {
    localStorage.setItem(
      "dags-saved-views",
      JSON.stringify([{ name: "Successful runs", search: "state=success" }]),
    );

    render(<SavedViewsMenu />, { wrapper: createWrapper(["/dags"]) });
    await openMenu();

    fireEvent.click(screen.getByLabelText("savedViews.setDefault"));
    await waitFor(() => {
      expect(JSON.parse(localStorage.getItem("dags-saved-views-default") ?? "null")).toBe("Successful runs");
    });

    fireEvent.click(screen.getByLabelText("savedViews.unsetDefault"));
    await waitFor(() => {
      expect(JSON.parse(localStorage.getItem("dags-saved-views-default") ?? "null")).toBeNull();
    });
  });

  it("clears the stored default when the default view is deleted", async () => {
    localStorage.setItem(
      "dags-saved-views",
      JSON.stringify([{ name: "Successful runs", search: "state=success" }]),
    );
    localStorage.setItem("dags-saved-views-default", JSON.stringify("Successful runs"));

    // A non-bare URL so the default isn't auto-restored before we delete it.
    render(<SavedViewsMenu />, { wrapper: createWrapper(["/dags?state=running"]) });
    await openMenu();

    fireEvent.click(screen.getByLabelText("Delete view"));
    fireEvent.click(await screen.findByTestId("delete-confirm-button"));

    await waitFor(() => {
      expect(JSON.parse(localStorage.getItem("dags-saved-views") ?? "[]")).toEqual([]);
    });
    expect(JSON.parse(localStorage.getItem("dags-saved-views-default") ?? "null")).toBeNull();
  });
});
