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

import { PresetFiltersMenu } from "./PresetFiltersMenu";

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
  fireEvent.click(screen.getByTestId("preset-filters-button"));

  return screen.findByTestId("preset-filter-name");
};

afterEach(() => {
  cleanup();
  localStorage.clear();
});

describe("PresetFiltersMenu", () => {
  it("shows an empty state when no preset filters are saved", async () => {
    render(<PresetFiltersMenu />, { wrapper: createWrapper() });
    await openMenu();

    expect(screen.getByText("presetFilters.empty")).toBeInTheDocument();
  });

  it("saves the current preset from the URL, dropping pagination and keeping the sort", async () => {
    render(<PresetFiltersMenu />, {
      wrapper: createWrapper(["/dags?state=running&sort=-start_date&offset=40"]),
    });
    const input = await openMenu();

    fireEvent.change(input, { target: { value: "Running runs" } });
    fireEvent.click(screen.getByTestId("preset-filter-save"));

    await waitFor(() => {
      expect(JSON.parse(localStorage.getItem("dags-preset-filters") ?? "[]")).toEqual([
        { name: "Running runs", search: "state=running&sort=-start_date" },
      ]);
    });
    expect(input).toHaveValue("");
  });

  it("does not save a preset with a blank name", async () => {
    render(<PresetFiltersMenu />, { wrapper: createWrapper(["/dags?state=running"]) });
    const input = await openMenu();

    fireEvent.change(input, { target: { value: "   " } });

    expect(screen.getByTestId("preset-filter-save")).toBeDisabled();
  });

  it("disables save on a bare page, even with a stored sort and a typed name", async () => {
    localStorage.setItem("dags-table-sort", JSON.stringify([{ desc: true, id: "run_after" }]));

    render(<PresetFiltersMenu />, { wrapper: createWrapper(["/dags"]) });
    const input = await openMenu();

    fireEvent.change(input, { target: { value: "My preset" } });

    expect(screen.getByTestId("preset-filter-save")).toBeDisabled();
  });

  it("overwrites an existing preset with the same name", async () => {
    localStorage.setItem(
      "dags-preset-filters",
      JSON.stringify([{ name: "Running runs", search: "state=queued" }]),
    );

    render(<PresetFiltersMenu />, { wrapper: createWrapper(["/dags?state=running"]) });
    const input = await openMenu();

    fireEvent.change(input, { target: { value: "Running runs" } });
    fireEvent.click(screen.getByTestId("preset-filter-save"));

    await waitFor(() => {
      expect(JSON.parse(localStorage.getItem("dags-preset-filters") ?? "[]")).toEqual([
        { name: "Running runs", search: "state=running" },
      ]);
    });
  });

  it("restores a preset to the URL and the table sort", async () => {
    localStorage.setItem(
      "dags-preset-filters",
      JSON.stringify([{ name: "Successful runs", search: "state=success&sort=-run_after" }]),
    );

    render(<PresetFiltersMenu />, { wrapper: createWrapper(["/dags"]) });
    await openMenu();

    fireEvent.click(screen.getByText("Successful runs"));

    await waitFor(() => {
      expect(screen.getByTestId("location-search")).toHaveTextContent("state=success");
    });
    expect(screen.getByTestId("location-search")).toHaveTextContent("sort=-run_after");
    expect(JSON.parse(localStorage.getItem("dags-table-sort") ?? "[]")).toEqual([
      { desc: true, id: "run_after" },
    ]);
  });

  it("deletes a preset after confirming", async () => {
    localStorage.setItem(
      "dags-preset-filters",
      JSON.stringify([
        { name: "Successful runs", search: "state=success" },
        { name: "Failed runs", search: "state=failed" },
      ]),
    );

    render(<PresetFiltersMenu />, { wrapper: createWrapper(["/dags"]) });
    await openMenu();

    fireEvent.click(screen.getAllByLabelText("Delete preset filter")[0] as HTMLElement);
    fireEvent.click(await screen.findByTestId("delete-confirm-button"));

    await waitFor(() => {
      expect(JSON.parse(localStorage.getItem("dags-preset-filters") ?? "[]")).toEqual([
        { name: "Failed runs", search: "state=failed" },
      ]);
    });
  });

  it("keeps the preset when the delete is cancelled", async () => {
    localStorage.setItem(
      "dags-preset-filters",
      JSON.stringify([{ name: "Successful runs", search: "state=success" }]),
    );

    render(<PresetFiltersMenu />, { wrapper: createWrapper(["/dags"]) });
    await openMenu();

    fireEvent.click(screen.getByLabelText("Delete preset filter"));
    fireEvent.click(await screen.findByTestId("delete-cancel-button"));

    await waitFor(() => {
      expect(screen.queryByTestId("delete-confirm-button")).not.toBeInTheDocument();
    });
    expect(JSON.parse(localStorage.getItem("dags-preset-filters") ?? "[]")).toEqual([
      { name: "Successful runs", search: "state=success" },
    ]);
  });

  it("blocks save when the exact setup is already saved under another name", async () => {
    localStorage.setItem(
      "dags-preset-filters",
      JSON.stringify([{ name: "Running runs", search: "state=running" }]),
    );

    render(<PresetFiltersMenu />, { wrapper: createWrapper(["/dags?state=running"]) });
    const input = await openMenu();

    fireEvent.change(input, { target: { value: "Another name" } });

    expect(screen.getByTestId("preset-filter-save")).toBeDisabled();
  });

  it("restores the default preset automatically on a bare page load", async () => {
    localStorage.setItem(
      "dags-preset-filters",
      JSON.stringify([{ name: "Successful runs", search: "state=success&sort=-run_after" }]),
    );
    localStorage.setItem("dags-preset-filters-default", JSON.stringify("Successful runs"));

    render(<PresetFiltersMenu />, { wrapper: createWrapper(["/dags"]) });

    await waitFor(() => {
      expect(screen.getByTestId("location-search")).toHaveTextContent("state=success");
    });
    expect(screen.getByTestId("location-search")).toHaveTextContent("sort=-run_after");
    expect(JSON.parse(localStorage.getItem("dags-table-sort") ?? "[]")).toEqual([
      { desc: true, id: "run_after" },
    ]);
  });

  it("does not restore the default preset when the URL carries a non-default sort", async () => {
    localStorage.setItem(
      "dags-preset-filters",
      JSON.stringify([{ name: "Successful runs", search: "state=success" }]),
    );
    localStorage.setItem("dags-preset-filters-default", JSON.stringify("Successful runs"));

    render(<PresetFiltersMenu />, { wrapper: createWrapper(["/dags?sort=-run_after"]) });
    await openMenu();

    expect(screen.getByTestId("location-search")).toHaveTextContent("sort=-run_after");
    expect(screen.getByTestId("location-search")).not.toHaveTextContent("state=success");
  });

  it("does not restore the default preset when the page already has filters", async () => {
    localStorage.setItem(
      "dags-preset-filters",
      JSON.stringify([{ name: "Successful runs", search: "state=success" }]),
    );
    localStorage.setItem("dags-preset-filters-default", JSON.stringify("Successful runs"));

    render(<PresetFiltersMenu />, { wrapper: createWrapper(["/dags?state=running"]) });
    await openMenu();

    expect(screen.getByTestId("location-search")).toHaveTextContent("state=running");
    expect(screen.getByTestId("location-search")).not.toHaveTextContent("state=success");
  });

  it("toggles a preset as the default and back", async () => {
    localStorage.setItem(
      "dags-preset-filters",
      JSON.stringify([{ name: "Successful runs", search: "state=success" }]),
    );

    render(<PresetFiltersMenu />, { wrapper: createWrapper(["/dags"]) });
    await openMenu();

    fireEvent.click(screen.getByLabelText("presetFilters.setDefault"));
    await waitFor(() => {
      expect(JSON.parse(localStorage.getItem("dags-preset-filters-default") ?? "null")).toBe(
        "Successful runs",
      );
    });

    fireEvent.click(screen.getByLabelText("presetFilters.unsetDefault"));
    await waitFor(() => {
      expect(JSON.parse(localStorage.getItem("dags-preset-filters-default") ?? "null")).toBeNull();
    });
  });

  it("clears the stored default when the default preset is deleted", async () => {
    localStorage.setItem(
      "dags-preset-filters",
      JSON.stringify([{ name: "Successful runs", search: "state=success" }]),
    );
    localStorage.setItem("dags-preset-filters-default", JSON.stringify("Successful runs"));

    // A non-bare URL so the default isn't auto-restored before we delete it.
    render(<PresetFiltersMenu />, { wrapper: createWrapper(["/dags?state=running"]) });
    await openMenu();

    fireEvent.click(screen.getByLabelText("Delete preset filter"));
    fireEvent.click(await screen.findByTestId("delete-confirm-button"));

    await waitFor(() => {
      expect(JSON.parse(localStorage.getItem("dags-preset-filters") ?? "[]")).toEqual([]);
    });
    expect(JSON.parse(localStorage.getItem("dags-preset-filters-default") ?? "null")).toBeNull();
  });
});
