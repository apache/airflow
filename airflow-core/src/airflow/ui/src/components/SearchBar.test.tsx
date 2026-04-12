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
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { SearchBar } from "./SearchBar";

afterEach(() => {
  vi.useRealTimers();
});

describe("Test SearchBar", () => {
  it("Renders and clear button works", async () => {
    const onChange = vi.fn();

    render(<SearchBar defaultValue="" onChange={onChange} placeholder="Search Dags" />, {
      wrapper: Wrapper,
    });

    const input = screen.getByTestId("search-dags");

    expect(screen.queryByTestId("clear-search")).toBeNull();

    fireEvent.change(input, { target: { value: "search" } });

    await waitFor(() => expect((input as HTMLInputElement).value).toBe("search"));

    const clearButton = screen.getByTestId("clear-search");

    expect(clearButton).toBeDefined();

    fireEvent.click(clearButton);

    expect((input as HTMLInputElement).value).toBe("");
    expect(onChange).toHaveBeenCalledWith("");
  });

  it("cancels pending debounced changes when clearing", () => {
    vi.useFakeTimers();

    const onChange = vi.fn();

    render(<SearchBar defaultValue="" onChange={onChange} placeholder="Search Dags" />, {
      wrapper: Wrapper,
    });

    const input = screen.getByTestId("search-dags");

    fireEvent.change(input, { target: { value: "air" } });

    expect((input as HTMLInputElement).value).toBe("air");
    expect(onChange).not.toHaveBeenCalled();

    fireEvent.click(screen.getByTestId("clear-search"));

    expect((input as HTMLInputElement).value).toBe("");
    expect(onChange).toHaveBeenCalledTimes(1);
    expect(onChange).toHaveBeenNthCalledWith(1, "");

    act(() => {
      vi.advanceTimersByTime(200);
    });

    expect(onChange).toHaveBeenCalledTimes(1);
  });

  it("syncs input value when defaultValue changes", async () => {
    const onChange = vi.fn();
    const { rerender } = render(
      <SearchBar defaultValue="initial-search" onChange={onChange} placeholder="Search Dags" />,
      {
        wrapper: Wrapper,
      },
    );
    const input = screen.getByTestId("search-dags");

    expect((input as HTMLInputElement).value).toBe("initial-search");

    rerender(<SearchBar defaultValue="updated-search" onChange={onChange} placeholder="Search Dags" />);

    await waitFor(() => expect((input as HTMLInputElement).value).toBe("updated-search"));
  });

  it("does not override local typing when defaultValue rerenders unchanged", () => {
    const onChange = vi.fn();
    const { rerender } = render(
      <SearchBar defaultValue="initial" onChange={onChange} placeholder="Search Dags" />,
      {
        wrapper: Wrapper,
      },
    );
    const input = screen.getByTestId("search-dags");

    fireEvent.change(input, { target: { value: "user-typing" } });

    rerender(<SearchBar defaultValue="initial" onChange={onChange} placeholder="Search Dags" />);

    expect((input as HTMLInputElement).value).toBe("user-typing");
  });
});
