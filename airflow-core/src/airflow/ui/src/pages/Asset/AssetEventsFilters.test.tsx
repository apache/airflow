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
import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { AppWrapper } from "src/utils/AppWrapper";

// The assets mock handler serves two events for asset 1:
//   id 100 — producer_dag,           run manual__2025-01-10,    map index -1, no partition key
//   id 101 — scheduled_producer_dag, run scheduled__2025-01-11, map index 3,  partition key 2025-01-11
// The card shows the source Dag id rather than the run id, so assertions use the Dag id.
const route = (query: string) => `/assets/1?${query}`;

describe("Asset event filters", () => {
  it.each([
    { kept: "producer_dag", query: "source_run_id=manual__2025-01-10" },
    { kept: "scheduled_producer_dag", query: "source_map_index=3" },
    { kept: "scheduled_producer_dag", query: "partition_key=2025-01-11" },
  ])("narrows the events to one row for $query", async ({ kept, query }) => {
    render(<AppWrapper initialEntries={[route(query)]} />);

    await waitFor(() => expect(screen.getByText(kept)).toBeInTheDocument());
  });

  it("shows both events when no filter is applied", async () => {
    render(<AppWrapper initialEntries={["/assets/1"]} />);

    await waitFor(() => expect(screen.getByText("producer_dag")).toBeInTheDocument());
    expect(screen.getByText("scheduled_producer_dag")).toBeInTheDocument();
  });
});
