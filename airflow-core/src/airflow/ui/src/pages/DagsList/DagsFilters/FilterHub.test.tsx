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
import { describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { FilterHub } from "./FilterHub";
import type { DagsFilterModel } from "./types";

const createModel = (): DagsFilterModel => ({
  activeRunState: { onChange: vi.fn(), value: undefined },
  clearAll: vi.fn(),
  favorite: { onChange: vi.fn(), value: "all" },
  lastRunState: { onChange: vi.fn(), value: undefined },
  multiTeamEnabled: false,
  needsReview: { onChange: vi.fn(), value: "all" },
  owners: { onChange: vi.fn(), values: [] },
  paused: { onChange: vi.fn(), value: "all" },
  resetSuggestions: vi.fn(),
  tags: {
    hasError: false,
    hasNextPage: false,
    isLoading: false,
    matchMode: "any",
    onChange: vi.fn(),
    onInputChange: vi.fn(),
    onMatchModeChange: vi.fn(),
    onMenuScrollToBottom: vi.fn(),
    onMenuScrollToTop: vi.fn(),
    onRetry: vi.fn(),
    options: ["data-platform", "business-critical"],
    values: [],
  },
  teams: { onChange: vi.fn(), values: [] },
  timetableTypes: {
    hasError: false,
    hasNextPage: false,
    isLoading: false,
    onChange: vi.fn(),
    onInputChange: vi.fn(),
    onMenuScrollToBottom: vi.fn(),
    onMenuScrollToTop: vi.fn(),
    onRetry: vi.fn(),
    options: ["CronTriggerTimetable", "NullTimetable"],
    values: [],
  },
});

const createHub = (model: DagsFilterModel) => (
  <FilterHub
    advancedSearch={{ enabled: false, onToggle: vi.fn() }}
    model={model}
    onSearchChange={vi.fn()}
    searchValue="sales"
  />
);

const renderHub = (model: DagsFilterModel) => render(createHub(model), { wrapper: Wrapper });

describe("FilterHub", () => {
  it("keeps the name search full-width and summarizes active facets", () => {
    const baseModel = createModel();
    const model: DagsFilterModel = {
      ...baseModel,
      paused: { ...baseModel.paused, value: "false" },
      tags: { ...baseModel.tags, values: ["data-platform", "business-critical"] },
      timetableTypes: { ...baseModel.timetableTypes, values: ["CronTriggerTimetable"] },
    };

    renderHub(model);

    expect(screen.getByTestId("search-dags")).toHaveValue("sales");
    expect(screen.getByTestId("hub-filter-trigger")).toHaveTextContent("3");
    expect(screen.getByTestId("hub-active-filters")).toBeInTheDocument();
    expect(screen.getByTestId("hub-edit-paused")).toBeInTheDocument();
    expect(screen.getByTestId("hub-edit-tags")).toHaveTextContent("data-platform, business-critical");
    expect(screen.getByTestId("hub-edit-timetableTypes")).toHaveTextContent("CronTriggerTimetable");
  });

  it("removes one facet without changing the other filters", () => {
    const baseModel = createModel();
    const model: DagsFilterModel = {
      ...baseModel,
      paused: { ...baseModel.paused, value: "false" },
      timetableTypes: {
        ...baseModel.timetableTypes,
        values: ["CronTriggerTimetable", "NullTimetable"],
      },
    };

    renderHub(model);

    fireEvent.click(screen.getByTestId("hub-remove-timetableTypes"));

    expect(model.timetableTypes.onChange).toHaveBeenCalledOnce();
    expect(model.timetableTypes.onChange).toHaveBeenCalledWith([]);
    expect(model.paused.onChange).not.toHaveBeenCalled();
    expect(model.clearAll).not.toHaveBeenCalled();
  });

  it("opens the filter disclosure directly from an active chip", async () => {
    const baseModel = createModel();
    const model: DagsFilterModel = {
      ...baseModel,
      timetableTypes: { ...baseModel.timetableTypes, values: ["CronTriggerTimetable"] },
    };

    renderHub(model);

    fireEvent.click(screen.getByTestId("hub-edit-timetableTypes"));

    expect(await screen.findByLabelText("filters.timetableType")).toBeInTheDocument();
    expect(screen.getByText("common:dagDetails.schedule")).toBeInTheDocument();
    await waitFor(() => expect(screen.getByLabelText("filters.timetableType")).toHaveFocus());
  });

  it("forwards disclosure behavior through the custom Filters trigger", async () => {
    renderHub(createModel());

    fireEvent.click(screen.getByTestId("hub-filter-trigger"));

    expect(await screen.findByText("common:dagDetails.schedule")).toBeInTheDocument();
    expect(screen.getByTestId("hub-needs-review-all")).toHaveTextContent("dags:filters.needsReviewAll");
  });

  it("restores focus to the edited chip when the disclosure closes", async () => {
    const baseModel = createModel();
    const model: DagsFilterModel = {
      ...baseModel,
      timetableTypes: { ...baseModel.timetableTypes, values: ["CronTriggerTimetable"] },
    };

    renderHub(model);
    const editButton = screen.getByTestId("hub-edit-timetableTypes");

    fireEvent.click(editButton);
    fireEvent.click(await screen.findByRole("button", { name: "dags:filters.closeFilters" }));

    await waitFor(() => expect(editButton).toHaveFocus());
    expect(model.resetSuggestions).toHaveBeenCalledOnce();
  });

  it("clears a stale chip editor and restores focus to Filters after external removal", async () => {
    const baseModel = createModel();
    const model: DagsFilterModel = {
      ...baseModel,
      timetableTypes: { ...baseModel.timetableTypes, values: ["CronTriggerTimetable"] },
    };
    const { rerender } = renderHub(model);

    fireEvent.click(screen.getByTestId("hub-edit-timetableTypes"));
    const scheduleSection = (await screen.findByText("common:dagDetails.schedule")).closest("fieldset");

    expect(scheduleSection).toHaveAttribute("data-highlighted", "true");

    rerender(createHub(baseModel));

    expect(screen.getByText("common:dagDetails.schedule").closest("fieldset")).not.toHaveAttribute(
      "data-highlighted",
    );
    fireEvent.click(screen.getByRole("button", { name: "dags:filters.closeFilters" }));

    await waitFor(() => expect(screen.getByTestId("hub-filter-trigger")).toHaveFocus());
  });

  it("moves focus to the next chip after removing a facet", async () => {
    const baseModel = createModel();
    const model: DagsFilterModel = {
      ...baseModel,
      paused: { ...baseModel.paused, value: "false" },
      timetableTypes: { ...baseModel.timetableTypes, values: ["CronTriggerTimetable"] },
    };

    renderHub(model);
    fireEvent.click(screen.getByTestId("hub-remove-paused"));

    await waitFor(() => expect(screen.getByTestId("hub-edit-timetableTypes")).toHaveFocus());
  });

  it("represents and clears owner deep links", () => {
    const baseModel = createModel();
    const model: DagsFilterModel = {
      ...baseModel,
      owners: { ...baseModel.owners, values: ["airflow", "data-platform"] },
    };

    renderHub(model);

    expect(screen.getByTestId("hub-edit-owners")).toHaveTextContent("airflow, data-platform");
    fireEvent.click(screen.getByTestId("hub-remove-owners"));
    expect(model.owners.onChange).toHaveBeenCalledOnce();
    expect(model.owners.onChange).toHaveBeenCalledWith([]);
  });

  it("summarizes long multi-value facets", () => {
    const baseModel = createModel();
    const model: DagsFilterModel = {
      ...baseModel,
      tags: { ...baseModel.tags, values: ["one", "two", "three", "four"] },
    };

    renderHub(model);

    expect(screen.getByTestId("hub-edit-tags")).toHaveTextContent("one, two +2");
  });

  it("offers Clear filters whenever an effective facet is active", () => {
    const baseModel = createModel();
    const model: DagsFilterModel = {
      ...baseModel,
      paused: { ...baseModel.paused, value: "false" },
    };

    renderHub(model);

    fireEvent.click(screen.getByText("dags:filters.clearFilters"));

    expect(model.clearAll).toHaveBeenCalledOnce();
  });
});
