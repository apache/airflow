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
import { afterEach, describe, it, expect, vi } from "vitest";

import { dagsListHandler, defaultDags, drainingDag } from "src/mocks/handlers/dags";
import { server } from "src/mocks/server";
import { AppWrapper } from "src/utils/AppWrapper";

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

// Only the draining assertions need a fourth Dag, and every extra row is re-rendered by each
// test that leans on the default fixture, so it is served per-test rather than globally.

const withDrainingDag = () => server.use(dagsListHandler([...defaultDags, drainingDag]));

const addFilter = async (key: string) => {
  fireEvent.click(screen.getByTestId("add-filter-button"));
  fireEvent.click(await screen.findByTestId(`add-filter-${key}`));
};

const selectPillOption = async (key: string, value: string) => {
  fireEvent.click(await screen.findByTestId(`${key}-pill`));
  fireEvent.click(await screen.findByTestId(`${key}-filter`));
  fireEvent.click(await screen.findByTestId(`${key}-filter-${value}`));
};

describe("Paused filter with hide_paused_dags_by_default enabled", () => {
  afterEach(() => {
    mockConfig.multi_team = false;
  });

  it("defaults to showing only active dags", async () => {
    render(<AppWrapper initialEntries={["/dags"]} />);

    await waitFor(() => {
      expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument();
      expect(screen.queryByText("paused_dag")).not.toBeInTheDocument();
    });
  });

  it("keeps draining dags under the default, since they are not paused yet", async () => {
    withDrainingDag();
    render(<AppWrapper initialEntries={["/dags"]} />);

    await waitFor(() => {
      expect(screen.getByText("draining_dag")).toBeInTheDocument();
      expect(screen.queryByText("paused_dag")).not.toBeInTheDocument();
    });
  });

  it("shows the default as a pill rather than filtering invisibly", async () => {
    render(<AppWrapper initialEntries={["/dags"]} />);

    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument());

    const pill = await screen.findByTestId("paused-pill");

    expect(pill).toHaveTextContent("filters.paused.active");
  });

  it("restores the draining choice from a scheduling_state url", async () => {
    withDrainingDag();
    render(<AppWrapper initialEntries={["/dags?scheduling_state=draining"]} />);

    const pill = await screen.findByTestId("paused-pill");

    // Draining is narrower than not-paused, so the shared pill has to report the narrower one.
    expect(pill).toHaveTextContent("draining.label");
    await waitFor(() => expect(screen.getByText("draining_dag")).toBeInTheDocument());
    expect(screen.queryByText("tutorial_taskflow_api_success")).not.toBeInTheDocument();
  });

  it("offers one state pill rather than a separate scheduling state", async () => {
    render(<AppWrapper initialEntries={["/dags"]} />);

    await waitFor(() => expect(screen.getByTestId("paused-pill")).toBeInTheDocument());
    expect(screen.queryByTestId("scheduling_state-pill")).not.toBeInTheDocument();

    fireEvent.click(screen.getByTestId("add-filter-button"));

    expect(screen.queryByTestId("add-filter-scheduling_state")).not.toBeInTheDocument();
  });

  it("shows all dags after removing the paused filter", async () => {
    render(<AppWrapper initialEntries={["/dags"]} />);

    await waitFor(() => {
      expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument();
      expect(screen.queryByText("paused_dag")).not.toBeInTheDocument();
    });

    // There is no "All" option any more; removing the pill is how you ask for every Dag.
    fireEvent.click(screen.getByTestId("paused-pill-remove"));

    await waitFor(() => expect(screen.getByText("paused_dag")).toBeInTheDocument());
    expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument();
  });

  it("shows only paused dags after clicking Paused filter", async () => {
    render(<AppWrapper initialEntries={["/dags"]} />);

    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument());

    await selectPillOption("paused", "paused");
    await waitFor(() => expect(screen.getByText("paused_dag")).toBeInTheDocument());
    await waitFor(() => expect(screen.queryByText("tutorial_taskflow_api_success")).not.toBeInTheDocument());
  });

  it("filters dags by a timetable type picked from the menu", async () => {
    render(<AppWrapper initialEntries={["/dags"]} />);

    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument());
    expect(screen.getByText("tutorial_taskflow_api_failed")).toBeInTheDocument();

    await addFilter("timetable_type");

    const input = await screen.findByLabelText("filters.timetableType");

    // The editor takes focus a frame after opening; typing before that lands nowhere.
    await waitFor(() => expect(input).toHaveFocus());
    fireEvent.change(input, { target: { value: "Cron" } });

    expect(screen.queryByText("NullTimetable")).not.toBeInTheDocument();
    fireEvent.click(await screen.findByText("CronTriggerTimetable"));

    await waitFor(() => {
      expect(screen.queryByText("tutorial_taskflow_api_success")).not.toBeInTheDocument();
      expect(screen.getByText("tutorial_taskflow_api_failed")).toBeInTheDocument();
    });
  });

  it("shows every dag again once the timetable type filter is removed", async () => {
    render(<AppWrapper initialEntries={["/dags?timetable_type=CronTriggerTimetable"]} />);

    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_failed")).toBeInTheDocument());
    expect(screen.queryByText("tutorial_taskflow_api_success")).not.toBeInTheDocument();

    fireEvent.click(await screen.findByTestId("timetable_type-pill-remove"));

    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument());
    expect(screen.getByText("tutorial_taskflow_api_failed")).toBeInTheDocument();
  });

  it("restores timetable types from the URL", async () => {
    render(
      <AppWrapper
        initialEntries={["/dags?timetable_type=CronTriggerTimetable&timetable_type=NullTimetable"]}
      />,
    );

    await waitFor(() => {
      expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument();
      expect(screen.getByText("tutorial_taskflow_api_failed")).toBeInTheDocument();
    });
    // The collapsed pill lists its values as a single joined string.
    const pill = screen.getByTestId("timetable_type-pill");

    expect(pill).toHaveTextContent("CronTriggerTimetable");
    expect(pill).toHaveTextContent("NullTimetable");
  });

  it("ignores an empty timetable type from the URL", async () => {
    render(<AppWrapper initialEntries={["/dags?timetable_type="]} />);

    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument());
    expect(screen.getByText("tutorial_taskflow_api_failed")).toBeInTheDocument();
  });

  it("renders the team filter when multi-team is enabled", async () => {
    mockConfig.multi_team = true;

    render(<AppWrapper initialEntries={["/dags"]} />);

    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument());
    fireEvent.click(screen.getByTestId("add-filter-button"));

    expect(await screen.findByTestId("add-filter-teams")).toBeInTheDocument();
  });

  it("renders the preset filters menu", async () => {
    render(<AppWrapper initialEntries={["/dags"]} />);

    expect(await screen.findByTestId("preset-filters-button")).toBeInTheDocument();
  });
});

describe("Links written before draining moved into the pill", () => {
  it.each([
    {
      entries: "/dags?paused=false",
      expected: "filters.paused.active",
      shown: "tutorial_taskflow_api_success",
    },
    { entries: "/dags?paused=true", expected: "filters.paused.paused", shown: "paused_dag" },
    { entries: "/dags?scheduling_state=paused", expected: "filters.paused.paused", shown: "paused_dag" },
  ])("restores $entries onto the Paused state pill", async ({ entries, expected, shown }) => {
    render(<AppWrapper initialEntries={[entries]} />);

    const pill = await screen.findByTestId("paused-pill");

    expect(pill).toHaveTextContent(expected);
    await waitFor(() => expect(screen.getByText(shown)).toBeInTheDocument());
  });

  it("ignores scheduling_state=active rather than filtering with no pill to explain it", async () => {
    render(<AppWrapper initialEntries={["/dags?scheduling_state=active"]} />);

    // No choice means "not paused and not draining", so the param is inert instead of narrowing
    // the list invisibly.
    await waitFor(() => expect(screen.getByText("paused_dag")).toBeInTheDocument());
    expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument();
    expect(screen.queryByTestId("paused-pill")).not.toBeInTheDocument();
  });
});
