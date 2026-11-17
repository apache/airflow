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
import { MemoryRouter, useLocation } from "react-router-dom";
import { afterEach, describe, it, expect, vi } from "vitest";

import { TimezoneProvider } from "src/context/timezone";
import { brokenDag, dagsListHandler, defaultDags, drainingDag } from "src/mocks/handlers/dags";
import { server } from "src/mocks/server";
import { AppWrapper, preloadAppRoutes } from "src/utils/AppWrapper";
import { BaseWrapper } from "src/utils/Wrapper";

import { usePausedDefault } from "../usePausedDefault";
import { DagsFilters } from "./DagsFilters";

await preloadAppRoutes(["/dags"]);

const mockConfig: Record<string, unknown> = {
  auto_refresh_interval: 3,
  default_wrap: false,
  enable_swagger_ui: true,
  hide_paused_dags_by_default: true,
  instance_name: "Airflow",
  multi_team: false,
  page_size: 15,
  require_confirmation_dag_change: false,
  test_connection: "Disabled",
};

vi.mock("src/queries/useConfig", () => ({
  useConfig: (key: string) => mockConfig[key],
}));

afterEach(() => {
  mockConfig.hide_paused_dags_by_default = true;
  mockConfig.multi_team = false;
});

// DagsList seeds the paused default into the URL, so render the bar with it the same way.
const FiltersWithDefault = () => {
  usePausedDefault();

  return <DagsFilters />;
};

const LocationSearch = () => <output data-testid="location-search">{useLocation().search}</output>;

/** Renders the filter bar alone: what it shows and what it writes to the URL, without the list. */
const renderFilters = (entry = "/dags") =>
  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={[entry]}>
        <TimezoneProvider>
          <FiltersWithDefault />
          <LocationSearch />
        </TimezoneProvider>
      </MemoryRouter>
    </BaseWrapper>,
  );

const getSearchParams = () => new URLSearchParams(screen.getByTestId("location-search").textContent);

const addFilter = async (key: string) => {
  fireEvent.click(screen.getByTestId("add-filter-button"));
  fireEvent.click(await screen.findByTestId(`add-filter-${key}`));
};

const selectPillOption = async (key: string, value: string) => {
  fireEvent.click(await screen.findByTestId(`${key}-pill`));
  fireEvent.click(await screen.findByTestId(`${key}-filter`));
  fireEvent.click(await screen.findByTestId(`${key}-filter-${value}`));
};

describe("Paused state pill", () => {
  it("writes the active default to the URL when hide_paused_dags_by_default is on", async () => {
    renderFilters();

    // Shown as a pill rather than filtering invisibly.
    expect(await screen.findByTestId("paused-pill")).toHaveTextContent("filters.paused.active");
    expect(getSearchParams().get("paused")).toBe("false");
  });

  it.each([
    { entry: "/dags?paused=false", expected: "filters.paused.active" },
    { entry: "/dags?paused=true", expected: "filters.paused.paused" },
    // Draining is narrower than not-paused, so the shared pill has to report the narrower one.
    { entry: "/dags?scheduling_state=draining", expected: "draining.label" },
    // The dashboard's Active Dags link.
    { entry: "/dags?scheduling_state=active", expected: "stats.activeDags" },
    // Links written before draining moved into the pill.
    { entry: "/dags?scheduling_state=paused", expected: "filters.paused.paused" },
  ])("restores $entry onto the pill without replacing it with the default", async ({ entry, expected }) => {
    renderFilters(entry);

    expect(await screen.findByTestId("paused-pill")).toHaveTextContent(expected);
    expect(getSearchParams().toString()).toBe(new URL(entry, "http://localhost").searchParams.toString());
  });

  it("offers one state pill rather than a separate scheduling state", async () => {
    renderFilters();

    await screen.findByTestId("paused-pill");
    expect(screen.queryByTestId("scheduling_state-pill")).not.toBeInTheDocument();

    fireEvent.click(screen.getByTestId("add-filter-button"));

    expect(screen.queryByTestId("add-filter-scheduling_state")).not.toBeInTheDocument();
  });

  it("drops the filter from the URL when the pill is removed", async () => {
    renderFilters();

    // There is no "All" option any more; removing the pill is how you ask for every Dag.
    fireEvent.click(await screen.findByTestId("paused-pill-remove"));

    // The pill goes at once; the bar writes the URL after a short debounce.
    await waitFor(() => expect(getSearchParams().has("paused")).toBe(false));
    expect(screen.queryByTestId("paused-pill")).not.toBeInTheDocument();
  });

  it("writes each choice to the URL param that backs it", async () => {
    renderFilters("/dags?scheduling_state=active");

    await selectPillOption("paused", "paused");
    await waitFor(() => expect(getSearchParams().get("paused")).toBe("true"));
    expect(getSearchParams().has("scheduling_state")).toBe(false);

    await selectPillOption("paused", "not_paused");
    await waitFor(() => expect(getSearchParams().get("paused")).toBe("false"));

    await selectPillOption("paused", "active");
    await waitFor(() => expect(getSearchParams().get("scheduling_state")).toBe("active"));
    expect(getSearchParams().has("paused")).toBe(false);
  });
});

describe("Timetable type pill", () => {
  it("writes a type picked from the menu to the URL", async () => {
    renderFilters();

    await addFilter("timetable_type");

    const input = await screen.findByLabelText("filters.timetableType");

    // The editor takes focus a frame after opening; typing before that lands nowhere.
    await waitFor(() => expect(input).toHaveFocus());
    fireEvent.change(input, { target: { value: "Cron" } });

    expect(screen.queryByText("NullTimetable")).not.toBeInTheDocument();
    fireEvent.click(await screen.findByText("CronTriggerTimetable"));

    await waitFor(() => expect(getSearchParams().getAll("timetable_type")).toEqual(["CronTriggerTimetable"]));
  });

  it("restores several types from the URL", async () => {
    renderFilters("/dags?timetable_type=CronTriggerTimetable&timetable_type=NullTimetable");

    // The collapsed pill lists its values as a single joined string.
    const pill = await screen.findByTestId("timetable_type-pill");

    expect(pill).toHaveTextContent("CronTriggerTimetable");
    expect(pill).toHaveTextContent("NullTimetable");
  });

  it("drops the types from the URL when the pill is removed", async () => {
    renderFilters("/dags?timetable_type=CronTriggerTimetable");

    fireEvent.click(await screen.findByTestId("timetable_type-pill-remove"));

    await waitFor(() => expect(getSearchParams().has("timetable_type")).toBe(false));
  });
});

describe("Other filters", () => {
  it("offers the team filter when multi-team is enabled", async () => {
    mockConfig.multi_team = true;
    renderFilters();

    fireEvent.click(screen.getByTestId("add-filter-button"));

    expect(await screen.findByTestId("add-filter-teams")).toBeInTheDocument();
  });

  it("renders the preset filters menu", async () => {
    renderFilters();

    expect(await screen.findByTestId("preset-filters-button")).toBeInTheDocument();
  });
});

// Without the paused default, so that the list filters only on the param under test.
const renderPage = (entry: string) => {
  mockConfig.hide_paused_dags_by_default = false;
  render(<AppWrapper initialEntries={[entry]} />);
};

// The whole page, only to check that the list asks the API for what the URL holds.
describe("Dags list filtered from the URL", () => {
  it("sends the paused default on every dags request, including the first", async () => {
    const urls: Array<URL> = [];
    const onRequest = ({ request }: { request: Request }) => {
      urls.push(new URL(request.url));
    };

    server.events.on("request:start", onRequest);
    try {
      render(<AppWrapper initialEntries={["/dags"]} />);

      await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument());
    } finally {
      server.events.removeListener("request:start", onRequest);
    }

    const dagsRequests = urls.filter((url) => url.pathname === "/ui/dags");

    expect(dagsRequests).toHaveLength(1);
    expect(dagsRequests.every((url) => url.searchParams.get("paused") === "false")).toBe(true);
  });

  it.each([
    { entry: "/dags?paused=true", hidden: "tutorial_taskflow_api_success", shown: "paused_dag" },
    {
      entry: "/dags?scheduling_state=draining",
      hidden: "tutorial_taskflow_api_success",
      shown: "draining_dag",
    },
    {
      entry: "/dags?timetable_type=CronTriggerTimetable",
      hidden: "tutorial_taskflow_api_success",
      shown: "tutorial_taskflow_api_failed",
    },
    { entry: "/dags?has_import_errors=true", hidden: "tutorial_taskflow_api_success", shown: "broken_dag" },
    { entry: "/dags?has_import_errors=false", hidden: "broken_dag", shown: "tutorial_taskflow_api_success" },
  ])("shows only the matching Dags for $entry", async ({ entry, hidden, shown }) => {
    server.use(dagsListHandler([...defaultDags, drainingDag, brokenDag]));
    renderPage(entry);

    expect(await screen.findByText(shown)).toBeInTheDocument();
    expect(screen.queryByText(hidden)).not.toBeInTheDocument();
  });

  it("ignores an empty timetable type", async () => {
    renderPage("/dags?timetable_type=");

    expect(await screen.findByText("tutorial_taskflow_api_success")).toBeInTheDocument();
    expect(screen.getByText("tutorial_taskflow_api_failed")).toBeInTheDocument();
  });
});
