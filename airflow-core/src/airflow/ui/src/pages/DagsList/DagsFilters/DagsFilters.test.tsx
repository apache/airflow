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

describe("Paused filter with hide_paused_dags_by_default enabled", () => {
  afterEach(() => {
    mockConfig.multi_team = false;
  });

  it("defaults to showing only active dags", async () => {
    render(<AppWrapper initialEntries={["/dags"]} />);

    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument());
    expect(screen.queryByText("paused_dag")).not.toBeInTheDocument();
  });

  it("shows all dags after clicking All filter", async () => {
    render(<AppWrapper initialEntries={["/dags"]} />);

    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument());
    expect(screen.queryByText("paused_dag")).not.toBeInTheDocument();

    // PausedFilter is the only filter using the "All" (filters.paused.all) label.
    screen.getByText("filters.paused.all").click();
    await waitFor(() => expect(screen.getByText("paused_dag")).toBeInTheDocument());
    expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument();
  });

  it("shows only paused dags after clicking Paused filter", async () => {
    render(<AppWrapper initialEntries={["/dags"]} />);

    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument());

    screen.getByText("filters.paused.paused").click();
    await waitFor(() => expect(screen.getByText("paused_dag")).toBeInTheDocument());
    await waitFor(() => expect(screen.queryByText("tutorial_taskflow_api_success")).not.toBeInTheDocument());
  });

  it("filters and clears dags by timetable types", async () => {
    render(<AppWrapper initialEntries={["/dags"]} />);

    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument());
    expect(screen.getByText("tutorial_taskflow_api_failed")).toBeInTheDocument();

    const timetableTypeFilter = screen.getByLabelText("filters.timetableType");

    fireEvent.change(timetableTypeFilter, { target: { value: "Cron" } });
    const cronTriggerTimetable = await screen.findByText("CronTriggerTimetable");

    expect(screen.queryByText("NullTimetable")).not.toBeInTheDocument();
    fireEvent.click(cronTriggerTimetable);

    await waitFor(() => {
      expect(screen.queryByText("tutorial_taskflow_api_success")).not.toBeInTheDocument();
      expect(screen.getByText("tutorial_taskflow_api_failed")).toBeInTheDocument();
    });

    fireEvent.change(timetableTypeFilter, { target: { value: "Null" } });
    fireEvent.click(await screen.findByText("NullTimetable"));

    await waitFor(() => {
      expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument();
      expect(screen.getByText("tutorial_taskflow_api_failed")).toBeInTheDocument();
    });

    fireEvent.keyDown(timetableTypeFilter, { code: "Backspace", key: "Backspace" });

    await waitFor(() => {
      expect(screen.queryByText("tutorial_taskflow_api_success")).not.toBeInTheDocument();
      expect(screen.getByText("tutorial_taskflow_api_failed")).toBeInTheDocument();
    });

    fireEvent.keyDown(timetableTypeFilter, { code: "Backspace", key: "Backspace" });

    await waitFor(() => {
      expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument();
      expect(screen.getByText("tutorial_taskflow_api_failed")).toBeInTheDocument();
    });
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
    expect(screen.getByText("CronTriggerTimetable")).toBeInTheDocument();
    expect(screen.getByText("NullTimetable")).toBeInTheDocument();
  });

  it("ignores an empty timetable type from the URL", async () => {
    render(<AppWrapper initialEntries={["/dags?timetable_type="]} />);

    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument());
    expect(screen.getByText("tutorial_taskflow_api_failed")).toBeInTheDocument();
  });

  it("renders the team filter when multi-team is enabled", async () => {
    mockConfig.multi_team = true;

    render(<AppWrapper initialEntries={["/dags"]} />);

    expect(await screen.findByLabelText("dagDetails.team")).toBeInTheDocument();
  });
});
