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
import { describe, expect, it, vi } from "vitest";

import { ChakraWrapper } from "src/utils/ChakraWrapper.tsx";

import { FilterBar } from "./FilterBar";
import type { FilterConfig } from "./types";

const configs: Array<FilterConfig> = [
  {
    key: "state",
    label: "State",
    type: "text",
  },
];

const getPillText = (value: string) => {
  const matches = screen.getAllByText((_, element) => element?.textContent === `State: ${value}`);

  return matches[matches.length - 1];
};

const queryPillText = (value: string) =>
  screen.queryAllByText((_, element) => element?.textContent === `State: ${value}`).at(-1);

describe("FilterBar", () => {
  it("updates existing pills when initial values change", async () => {
    const onFiltersChange = vi.fn();
    const { rerender } = render(
      <FilterBar configs={configs} initialValues={{ state: "queued" }} onFiltersChange={onFiltersChange} />,
      { wrapper: ChakraWrapper },
    );

    expect(getPillText("queued")).toBeInTheDocument();

    rerender(
      <FilterBar configs={configs} initialValues={{ state: "running" }} onFiltersChange={onFiltersChange} />,
    );

    await waitFor(() => expect(getPillText("running")).toBeInTheDocument());
    expect(queryPillText("queued")).toBeUndefined();
  });
});
