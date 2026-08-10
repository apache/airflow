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
import { cleanup, render, screen } from "@testing-library/react";
import type { PropsWithChildren } from "react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import { FilterBar } from "./FilterBar";

const wrapper = ({ children }: PropsWithChildren) => (
  <BaseWrapper>
    <MemoryRouter initialEntries={["/dags"]}>{children}</MemoryRouter>
  </BaseWrapper>
);

afterEach(cleanup);

describe("FilterBar preset filters", () => {
  it("shows the preset filters control by default", () => {
    render(<FilterBar configs={[]} onFiltersChange={vi.fn()} />, { wrapper });

    expect(screen.getByTestId("preset-filters-button")).toBeInTheDocument();
  });

  it("hides the preset filters control when showPresetFilters is false", () => {
    render(<FilterBar configs={[]} onFiltersChange={vi.fn()} showPresetFilters={false} />, { wrapper });

    expect(screen.queryByTestId("preset-filters-button")).not.toBeInTheDocument();
  });
});
