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
import { Button, Input } from "@chakra-ui/react";
import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { useRef, useState } from "react";
import { describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { SearchAndFilter } from "./SearchAndFilter";

const labels = {
  activeFilterCount: (count: number) => `${count} active filters`,
  clearFilters: "Clear filters",
  closeFilters: "Close filters",
  filterButton: "Filters",
  filterTitle: "Filter workflows",
};

const Harness = ({ onClearFilters = vi.fn() }: { readonly onClearFilters?: () => void }) => {
  const [open, setOpen] = useState(false);
  const firstFilterRef = useRef<HTMLButtonElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);

  return (
    <SearchAndFilter
      activeFilterCount={1}
      activeFilters={<Button>Edit owner: airflow</Button>}
      initialFocusEl={() => firstFilterRef.current}
      labels={labels}
      onClearFilters={onClearFilters}
      onOpenChange={setOpen}
      open={open}
      searchControl={<Input aria-label="Search workflows" />}
      triggerRef={triggerRef}
    >
      <Button ref={firstFilterRef}>First filter</Button>
    </SearchAndFilter>
  );
};

describe("SearchAndFilter", () => {
  it("composes page controls and exposes the active count in the trigger name", () => {
    render(<Harness />, { wrapper: Wrapper });

    expect(screen.getByRole("textbox", { name: "Search workflows" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Filters, 1 active filters" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Edit owner: airflow" })).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent("1 active filters");
  });

  it("moves focus into the disclosure and restores it after Escape", async () => {
    render(<Harness />, { wrapper: Wrapper });
    const trigger = screen.getByRole("button", { name: "Filters, 1 active filters" });

    fireEvent.click(trigger);

    const firstFilter = await screen.findByRole("button", { name: "First filter" });

    await waitFor(() => expect(firstFilter).toHaveFocus());
    fireEvent.keyDown(firstFilter, { key: "Escape" });
    await waitFor(() => expect(trigger).toHaveFocus());
  });

  it("clears active filters and returns focus to the trigger", () => {
    const onClearFilters = vi.fn();

    render(<Harness onClearFilters={onClearFilters} />, { wrapper: Wrapper });
    const trigger = screen.getByRole("button", { name: "Filters, 1 active filters" });

    fireEvent.click(screen.getByRole("button", { name: "Clear filters" }));

    expect(onClearFilters).toHaveBeenCalledOnce();
    expect(trigger).toHaveFocus();
  });
});
