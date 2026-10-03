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
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, useLocation } from "react-router-dom";
import { describe, expect, it } from "vitest";

import type { LoopSummaryResponse } from "openapi/requests/types.gen";

import { BaseWrapper } from "src/utils/Wrapper";

import { IterationSelect } from "./IterationSelect";

const summary: LoopSummaryResponse = {
  dag_id: "dag",
  group_id: "loop",
  iterations: [0, 1].map((index) => ({ end_date: null, index, start_date: null, state: "success" })),
  iterations_ran: 2,
  loop_region_id: "region-one",
  loop_regions: [{ parent_iterations: [], region_id: "region-one" }],
  max_iterations: 5,
  run_id: "run",
  status: "running",
};

const Location = () => <output data-testid="location">{useLocation().search}</output>;
const View = ({ data }: { readonly data: LoopSummaryResponse }) => (
  <BaseWrapper>
    <MemoryRouter initialEntries={["/?iteration=all&map_index=7"]}>
      <IterationSelect summary={data} />
      <Location />
    </MemoryRouter>
  </BaseWrapper>
);

describe("IterationSelect", () => {
  it("defaults to the newest iteration without changing mapped slot selection", async () => {
    render(
      <BaseWrapper>
        <MemoryRouter initialEntries={["/?map_index=7"]}>
          <IterationSelect summary={summary} />
          <Location />
        </MemoryRouter>
      </BaseWrapper>,
    );
    await waitFor(() => expect(screen.getByTestId("location")).toHaveTextContent("iteration=1"));
    expect(screen.getByTestId("location")).toHaveTextContent("map_index=7");
    expect(screen.getByTestId("location")).toHaveTextContent("loop_region_id=region-one");
  });

  it("keeps All selected when a new iteration is created", () => {
    const { rerender } = render(<View data={summary} />);

    rerender(
      <View
        data={{ ...summary, iterations: [...summary.iterations, { ...summary.iterations[0], index: 2 }] }}
      />,
    );
    expect(screen.getByTestId("location")).toHaveTextContent("iteration=all&map_index=7");
  });

  it("selects all iterations without removing the mapped slot filter", async () => {
    render(
      <BaseWrapper>
        <MemoryRouter initialEntries={["/?iteration=1&map_index=7&cursor=old"]}>
          <IterationSelect summary={summary} />
          <Location />
        </MemoryRouter>
      </BaseWrapper>,
    );
    fireEvent.click(screen.getByTestId("loop-iteration-select"));
    fireEvent.click(await screen.findByRole("option", { name: "loop.filter.all" }));
    await waitFor(() => expect(screen.getByTestId("location")).toHaveTextContent("iteration=all"));
    expect(screen.getByTestId("location")).toHaveTextContent("map_index=7");
    expect(screen.getByTestId("location")).not.toHaveTextContent("cursor");
  });
  it("replaces a cleared later iteration with the newest existing iteration and resets paging", async () => {
    render(
      <BaseWrapper>
        <MemoryRouter initialEntries={["/?iteration=9&cursor=old"]}>
          <IterationSelect summary={summary} />
          <Location />
        </MemoryRouter>
      </BaseWrapper>,
    );
    await waitFor(() => expect(screen.getByTestId("location")).toHaveTextContent("iteration=1"));
    expect(screen.getByTestId("location")).not.toHaveTextContent("cursor");
  });
});
