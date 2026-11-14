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
import { render, screen } from "@testing-library/react";
import type { Job, JobCollectionResponse } from "openapi/requests/types.gen";
import { MemoryRouter } from "react-router-dom";
import { expect, it, vi } from "vitest";

import { JobsPage } from "src/pages/JobsPage";

const query = vi.hoisted((): { data: JobCollectionResponse } => ({
  data: { jobs: [], total_entries: 0 },
}));

vi.mock("openapi/queries", () => ({ useJobs: () => query }));
vi.mock("@chakra-ui/react", () => ({
  Box: "div",
  HStack: "div",
  Text: "span",
  Table: { Root: "table", Header: "thead", Row: "tr", ColumnHeader: "th", Body: "tbody", Cell: "td" },
}));
vi.mock("src/components/ui", () => ({
  Select: { Root: "div", Trigger: "div", ValueText: () => null, Content: "div", Item: "div" },
}));
vi.mock("src/components/SearchBar", () => ({ SearchBar: () => null }));
vi.mock("src/components/ErrorAlert", () => ({ ErrorAlert: () => null }));
vi.mock("src/components/StateBadge", () => ({ StateBadge: "span" }));
vi.mock("src/constants", () => ({ jobStateOptions: { items: [] } }));
vi.mock("src/utils", () => ({ autoRefreshInterval: 1000 }));

it("keeps same-coordinate attempts distinct when jobs reorder or disappear", () => {
  const error = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const coordinates: Omit<Job, "task_instance_id" | "edge_worker"> = {
    dag_id: "dag",
    task_id: "task",
    run_id: "run",
    map_index: -1,
    try_number: 1,
    state: "running",
    queue: "default",
  };
  const first: Job = {
    ...coordinates,
    task_instance_id: "00000000-0000-0000-0000-000000000001",
    edge_worker: "first",
  };
  const second: Job = {
    ...coordinates,
    task_instance_id: "00000000-0000-0000-0000-000000000002",
    edge_worker: "second",
  };
  const legacy: Job = { ...coordinates, task_instance_id: "", edge_worker: "legacy" };
  query.data = { jobs: [first, second, legacy], total_entries: 3 };
  const page = (
    <MemoryRouter>
      <JobsPage />
    </MemoryRouter>
  );
  const { rerender } = render(page);
  const rowText = () =>
    screen
      .getAllByRole("row")
      .slice(1)
      .map((row) => row.textContent);

  expect(rowText()).toEqual([
    expect.stringContaining("first"),
    expect.stringContaining("second"),
    expect.stringContaining("legacy"),
  ]);
  query.data = { jobs: [second, first, legacy], total_entries: 3 };
  rerender(
    <MemoryRouter>
      <JobsPage />
    </MemoryRouter>,
  );
  expect(rowText()).toEqual([
    expect.stringContaining("second"),
    expect.stringContaining("first"),
    expect.stringContaining("legacy"),
  ]);
  query.data = { jobs: [first, legacy], total_entries: 2 };
  rerender(
    <MemoryRouter>
      <JobsPage />
    </MemoryRouter>,
  );
  expect(rowText()).toEqual([expect.stringContaining("first"), expect.stringContaining("legacy")]);
  expect(error.mock.calls.filter(([message]) => String(message).includes("same key"))).toHaveLength(0);
});
