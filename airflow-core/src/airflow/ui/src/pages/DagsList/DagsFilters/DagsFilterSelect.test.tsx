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
import { fireEvent, render, screen } from "@testing-library/react";
import type { ComponentProps } from "react";
import { expect, it, vi } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import { DagsFilterSelect } from "./DagsFilterSelect";

const renderSelect = (props: Partial<ComponentProps<typeof DagsFilterSelect>> = {}) =>
  render(
    <DagsFilterSelect
      ariaLabel="Timetable suggestions"
      noOptionsMessage="No timetable types found"
      onChange={vi.fn()}
      options={[]}
      placeholder="Timetable type"
      values={[]}
      {...props}
    />,
    { wrapper: BaseWrapper },
  );

it("announces loading and more-page suggestion states", () => {
  const { rerender } = renderSelect({ isLoading: true });

  expect(screen.getByText("filters.suggestionsLoading")).toBeInTheDocument();

  rerender(
    <DagsFilterSelect
      ariaLabel="Timetable suggestions"
      hasNextPage
      noOptionsMessage="No timetable types found"
      onChange={vi.fn()}
      options={["CronTriggerTimetable"]}
      placeholder="Timetable type"
      values={[]}
    />,
  );
  expect(screen.getByText("filters.moreSuggestionsAvailable")).toBeInTheDocument();
});

it("announces errors and retries suggestion loading", () => {
  const onRetry = vi.fn();

  renderSelect({ hasError: true, onRetry });
  expect(screen.getByRole("alert")).toHaveTextContent("filters.suggestionsError");

  fireEvent.click(screen.getByRole("button", { name: "filters.retrySuggestions" }));
  expect(onRetry).toHaveBeenCalledOnce();
});

it("shows the supplied empty state when no suggestions match", () => {
  renderSelect();

  fireEvent.focus(screen.getByLabelText("Timetable suggestions"));
  fireEvent.keyDown(screen.getByLabelText("Timetable suggestions"), { key: "ArrowDown" });

  expect(screen.getByText("No timetable types found")).toBeInTheDocument();
});
