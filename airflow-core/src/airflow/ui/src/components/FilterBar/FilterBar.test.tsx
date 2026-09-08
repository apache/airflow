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
import { afterEach, describe, expect, it, vi } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import { FilterBar } from "./FilterBar";
import type { FilterConfig, FilterPluginProps } from "./types";

const wrapper = ({ children }: PropsWithChildren) => (
  <BaseWrapper>
    <MemoryRouter initialEntries={["/dags"]}>{children}</MemoryRouter>
  </BaseWrapper>
);

afterEach(cleanup);

describe("FilterBar preset filters", () => {
  it("shows the preset filters control by default", () => {
    render(<FilterBar configs={[]} onFiltersChange={vi.fn()} />, { wrapper });

    expect(screen.getByTestId("preset-filters-button")).toBeInTheDocument();
  });

  it("hides the preset filters control when showPresetFilters is false", () => {
    render(<FilterBar configs={[]} onFiltersChange={vi.fn()} showPresetFilters={false} />, { wrapper });

    expect(screen.queryByTestId("preset-filters-button")).not.toBeInTheDocument();
  });
});

const booleanConfig: FilterConfig = { key: "needs_review", label: "Needs Review", type: "boolean" };
const multiSelectConfig: FilterConfig = {
  key: "tags",
  label: "Tags",
  options: [
    { label: "alpha", value: "alpha" },
    { label: "beta", value: "beta" },
  ],
  type: "multiselect",
};

describe("FilterBar boolean filters", () => {
  it("activates in a single click from the add filter menu", async () => {
    const onFiltersChange = vi.fn();

    render(<FilterBar configs={[booleanConfig]} onFiltersChange={onFiltersChange} />, { wrapper });

    fireEvent.click(screen.getByTestId("add-filter-button"));
    fireEvent.click(await screen.findByTestId("add-filter-needs_review"));

    expect(screen.getByTestId("needs_review-pill")).toBeInTheDocument();
    await waitFor(() => expect(onFiltersChange).toHaveBeenCalledWith({ needs_review: "true" }));
  });

  it("renders label only, with no value suffix and no editor", () => {
    render(
      <FilterBar
        configs={[booleanConfig]}
        initialValues={{ needs_review: "true" }}
        onFiltersChange={vi.fn()}
      />,
      { wrapper },
    );

    expect(screen.getByTestId("needs_review-pill")).toHaveTextContent("Needs Review");
    expect(screen.getByTestId("needs_review-pill")).not.toHaveTextContent(":");
  });

  it("clears the filter when the pill is clicked", async () => {
    const onFiltersChange = vi.fn();

    render(
      <FilterBar
        configs={[booleanConfig]}
        initialValues={{ needs_review: "true" }}
        onFiltersChange={onFiltersChange}
      />,
      { wrapper },
    );

    fireEvent.click(screen.getByTestId("needs_review-pill"));

    expect(screen.queryByTestId("needs_review-pill")).not.toBeInTheDocument();
    await waitFor(() => expect(onFiltersChange).toHaveBeenCalledWith({}));
  });
});

describe("FilterBar multiselect filters", () => {
  it("renders a pill for each value from array initialValues", () => {
    render(
      <FilterBar
        configs={[multiSelectConfig]}
        initialValues={{ tags: ["alpha", "beta"] }}
        onFiltersChange={vi.fn()}
      />,
      { wrapper },
    );

    const pill = screen.getByTestId("tags-pill");

    expect(pill).toHaveTextContent("alpha");
    expect(pill).toHaveTextContent("beta");
  });

  it("does not render a pill for an empty array", () => {
    render(
      <FilterBar configs={[multiSelectConfig]} initialValues={{ tags: [] }} onFiltersChange={vi.fn()} />,
      { wrapper },
    );

    expect(screen.queryByTestId("tags-pill")).not.toBeInTheDocument();
  });
});

const CustomEditor = ({ filter }: FilterPluginProps) => <div>custom:{filter.config.key}</div>;

describe("FilterBar custom editors", () => {
  it("renders EditorComponent instead of dispatching on type", () => {
    render(
      <FilterBar
        configs={[{ ...multiSelectConfig, EditorComponent: CustomEditor }]}
        initialValues={{ tags: ["alpha"] }}
        onFiltersChange={vi.fn()}
      />,
      { wrapper },
    );

    expect(screen.getByText("custom:tags")).toBeInTheDocument();
    expect(screen.queryByTestId("tags-pill")).not.toBeInTheDocument();
  });
});

const textConfig: FilterConfig = { key: "dag_id", label: "Dag ID", type: "text" };

describe("FilterBar abandoned filters", () => {
  it("drops a filter left without a value when its editor closes", async () => {
    const onFiltersChange = vi.fn();

    render(<FilterBar configs={[textConfig]} onFiltersChange={onFiltersChange} />, { wrapper });

    fireEvent.click(screen.getByTestId("add-filter-button"));
    fireEvent.click(await screen.findByTestId("add-filter-dag_id"));

    // The pill focuses its input a frame after opening, and that focus cancels a pending blur.
    await waitFor(() => expect(screen.getByRole("textbox")).toHaveFocus());
    fireEvent.focusOut(screen.getByRole("textbox"));

    // Gone entirely rather than parked on the bar as a pill that filters nothing.
    await waitFor(() => expect(screen.queryByRole("textbox")).not.toBeInTheDocument());
    expect(screen.queryByTestId("dag_id-pill")).not.toBeInTheDocument();
  });

  it("keeps a filter that has a value when its editor closes", async () => {
    render(<FilterBar configs={[textConfig]} initialValues={{ dag_id: "abc" }} onFiltersChange={vi.fn()} />, {
      wrapper,
    });

    fireEvent.click(screen.getByTestId("dag_id-pill"));

    await waitFor(() => expect(screen.getByRole("textbox")).toHaveFocus());
    fireEvent.focusOut(screen.getByRole("textbox"));

    await waitFor(() => expect(screen.getByTestId("dag_id-pill")).toBeInTheDocument());
  });

  it("drops a filter left without a value when Escape is pressed", async () => {
    render(<FilterBar configs={[textConfig]} onFiltersChange={vi.fn()} />, { wrapper });

    fireEvent.click(screen.getByTestId("add-filter-button"));
    fireEvent.click(await screen.findByTestId("add-filter-dag_id"));
    fireEvent.keyDown(screen.getByRole("textbox"), { key: "Escape" });

    // Absent rather than merely collapsed: a collapsed pill also has no textbox.
    await waitFor(() => expect(screen.queryByRole("textbox")).not.toBeInTheDocument());
    expect(screen.queryByTestId("dag_id-pill")).not.toBeInTheDocument();
  });
});

describe("FilterBar text filter input testid", () => {
  it("exposes the actively-editing pill's input via a stable, unique testid", async () => {
    render(<FilterBar configs={[textConfig]} onFiltersChange={vi.fn()} />, { wrapper });

    fireEvent.click(screen.getByTestId("add-filter-button"));
    fireEvent.click(await screen.findByTestId("add-filter-dag_id"));

    // Regression guard for #72433: e2e tests locate this input via `filter-pill-input`
    // rather than `page.locator("div").filter({ hasText })`, which matched any ancestor
    // whose descendant text contained the filter label and broke once the filter bar's
    // DOM was restructured. `getByTestId` throws if more than one match is found, so this
    // also proves the testid stays unique while a pill is being edited.
    const input = screen.getByTestId("filter-pill-input");

    expect(input).toBe(screen.getByRole("textbox"));
  });
});

describe("FilterBar keyboard handling", () => {
  it("leaves Enter to editors that use it to commit a value", async () => {
    render(<FilterBar configs={[multiSelectConfig]} onFiltersChange={vi.fn()} />, { wrapper });

    fireEvent.click(screen.getByTestId("add-filter-button"));
    fireEvent.click(await screen.findByTestId("add-filter-tags"));

    const input = document.querySelector('input[id^="react-select"]');

    expect(input).not.toBeNull();
    fireEvent.keyDown(input as Element, { key: "Enter" });

    // The pill must survive: react-select commits on Enter, and the filter used to be torn
    // down before that value landed.
    await waitFor(() => expect(document.querySelector('input[id^="react-select"]')).not.toBeNull());
  });
});
