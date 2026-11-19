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
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { AppWrapper } from "src/utils/AppWrapper";

describe("Task Instances date filters", () => {
  it("shows a pill for a logical_date link so the filter is visible and removable", async () => {
    render(<AppWrapper initialEntries={["/task_instances?logical_date_gte=2026-06-01T00%3A00%3A00Z"]} />);

    // A registered filter renders its pill from the URL, so the param can be seen, changed and
    // cleared. Honouring it without a pill would narrow the list with nothing explaining it, and
    // Reset only clears filters it knows about.
    const pill = await screen.findByTestId("logical_date_range-pill");

    expect(pill).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Remove .* filter/u })).toBeInTheDocument();
  });

  it("offers both run after and logical date in the add-filter menu", async () => {
    render(<AppWrapper initialEntries={["/task_instances"]} />);

    fireEvent.click(await screen.findByTestId("add-filter-button"));

    expect(await screen.findByTestId("add-filter-run_after_range")).toBeInTheDocument();
    expect(screen.getByTestId("add-filter-logical_date_range")).toBeInTheDocument();
  });
});
