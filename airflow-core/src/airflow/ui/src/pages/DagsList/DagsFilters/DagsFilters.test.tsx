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
import { afterEach, describe, it, expect, vi } from "vitest";

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

const addFilter = async (key: string) => {
  fireEvent.click(screen.getByTestId("add-filter-button"));
  fireEvent.click(await screen.findByTestId(`add-filter-${key}`));
};

const selectPillOption = async (key: string, value: string) => {
  fireEvent.click(screen.getByTestId(`${key}-pill`));
  fireEvent.click(await screen.findByTestId(`${key}-filter`));
  fireEvent.click(await screen.findByTestId(`${key}-filter-${value}`));
};

describe("Paused filter with hide_paused_dags_by_default enabled", () => {
  afterEach(() => {
    mockConfig.multi_team = false;
  });

  it("defaults to showing only active dags", async () => {
    render(<AppWrapper initialEntries={["/dags"]} />);

    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument());
    expect(screen.queryByText("paused_dag")).not.toBeInTheDocument();
  });

  it("shows the default as a pill rather than filtering invisibly", async () => {
    render(<AppWrapper initialEntries={["/dags"]} />);

    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument());

    const pill = await screen.findByTestId("paused-pill");

    expect(pill).toHaveTextContent("filters.paused.active");
  });

  it("shows all dags after removing the paused filter", async () => {
    render(<AppWrapper initialEntries={["/dags"]} />);

    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument());
    expect(screen.queryByText("paused_dag")).not.toBeInTheDocument();

    // There is no "All" option any more; removing the pill is how you ask for every Dag.
    fireEvent.click(within(screen.getByTestId("paused-pill")).getByRole("button"));

    await waitFor(() => expect(screen.getByText("paused_dag")).toBeInTheDocument());
    expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument();
  });

  it("shows only paused dags after clicking Paused filter", async () => {
    render(<AppWrapper initialEntries={["/dags"]} />);

    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument());

    await selectPillOption("paused", "true");
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

    fireEvent.click(within(await screen.findByTestId("timetable_type-pill")).getByRole("button"));

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

  it("renders the last run state as plain text in the pill, keeping badges in the menu", async () => {
    render(<AppWrapper initialEntries={["/dags"]} />);

    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument());

    await addFilter("last_dag_run_state");
    fireEvent.click(await screen.findByTestId("last_dag_run_state-filter-failed"));

    const pill = await screen.findByTestId("last_dag_run_state-pill");

    // The pill is fixed-height, so a badge rendered as the value gets clipped; the
    // plain state label is shown instead and badges stay inside the dropdown menu.
    expect(within(pill).queryByTestId("state-badge")).not.toBeInTheDocument();
    expect(pill).toHaveTextContent("states.failed");
  });

  it("renders the preset filters menu", async () => {
    render(<AppWrapper initialEntries={["/dags"]} />);

    expect(await screen.findByTestId("preset-filters-button")).toBeInTheDocument();
  });
});
