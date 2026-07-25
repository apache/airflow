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
import { render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import "../../../i18n/config";
import { RunStateScopeSelect, type RunStateScope } from "./RunStateScopeSelect";

const SCOPE_VALUES: ReadonlyArray<RunStateScope> = ["latest", "24", "168", "720", "any"];

const renderScope = (value: RunStateScope, onChange = vi.fn()) => {
  render(<RunStateScopeSelect dataTestId="scope" onChange={onChange} value={value} />, {
    wrapper: ({ children }) => (
      <BaseWrapper>
        <MemoryRouter>{children}</MemoryRouter>
      </BaseWrapper>
    ),
  });

  return onChange;
};

describe("RunStateScopeSelect", () => {
  it("offers the latest-run scope plus every time window", async () => {
    renderScope("latest");

    within(screen.getByTestId("scope")).getByRole("combobox").click();

    await waitFor(() => expect(screen.getByTestId("scope-latest")).toBeInTheDocument());
    for (const value of SCOPE_VALUES) {
      expect(screen.getByTestId(`scope-${value}`)).toBeInTheDocument();
    }
  });

  it("emits the hour value of the selected window", async () => {
    const onChange = renderScope("latest");

    within(screen.getByTestId("scope")).getByRole("combobox").click();
    await waitFor(() => screen.getByTestId("scope-168").click());

    expect(onChange).toHaveBeenCalledWith("168");
  });
});
