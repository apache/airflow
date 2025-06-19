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
import { describe, it, expect, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { SearchBar } from "./SearchBar";

describe("Test SearchBar", () => {
  it("Renders and clear button works", async () => {
    render(<SearchBar defaultValue="" onChange={vi.fn()} placeHolder="Search Dags" />, {
      wrapper: Wrapper,
    });

    const input = screen.getByTestId("search-dags");

    expect(screen.getByText("search.advanced")).toBeDefined();
    expect(screen.queryByTestId("clear-search")).toBeNull();

    fireEvent.change(input, { target: { value: "search" } });

    await waitFor(() => expect((input as HTMLInputElement).value).toBe("search"));

    const clearButton = screen.getByTestId("clear-search");

    expect(clearButton).toBeDefined();

    fireEvent.click(clearButton);

    expect((input as HTMLInputElement).value).toBe("");
  });
});
