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
import { render, screen, waitFor } from "@testing-library/react";
import { describe, it, expect, vi } from "vitest";

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
  it("defaults to showing only active dags", async () => {
    render(<AppWrapper initialEntries={["/dags"]} />);

    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument());
    expect(screen.queryByText("paused_dag")).not.toBeInTheDocument();
  });

  it("shows all dags after clicking All filter", async () => {
    render(<AppWrapper initialEntries={["/dags"]} />);

    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument());
    expect(screen.queryByText("paused_dag")).not.toBeInTheDocument();

    // There are two "All" buttons (StateFilters and PausedFilter).
    // The second one belongs to PausedFilter.
    const allButtons = screen.getAllByText("filters.paused.all");

    allButtons[1]?.click();
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
});
