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
import type { PropsWithChildren } from "react";

import "@testing-library/jest-dom";
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import { DagService } from "openapi/requests/services.gen";
import type * as ServicesGen from "openapi/requests/services.gen";

import { BaseWrapper } from "src/utils/Wrapper";

import { FilterBar } from "../FilterBar";
import type { FilterConfig } from "../types";
import { TagsFilter } from "./TagsFilter";

vi.mock("openapi/requests/services.gen", async (importOriginal) => {
  const mod = await importOriginal<typeof ServicesGen>();

  return {
    ...mod,
    DagService: { ...mod.DagService, getDagTags: vi.fn() },
  };
});

const wrapper = ({ children }: PropsWithChildren) => (
  <BaseWrapper>
    <MemoryRouter initialEntries={["/dags"]}>{children}</MemoryRouter>
  </BaseWrapper>
);

const tagsConfig: FilterConfig = {
  EditorComponent: TagsFilter,
  key: "tags",
  label: "Tags",
  matchModeKey: "tags_match_mode",
  supportsAdvancedSearch: true,
  type: "multiselect",
};

const openTagsEditor = async () => {
  fireEvent.click(screen.getByTestId("add-filter-button"));
  fireEvent.click(await screen.findByTestId("add-filter-tags"));
};

const typeIntoSelect = (value: string) => {
  const input = document.querySelector('input[id^="react-select"]');

  expect(input).not.toBeNull();
  fireEvent.change(input as Element, { target: { value } });
};

const getDagTagsMock = vi.mocked(DagService.getDagTags);

afterEach(() => {
  localStorage.clear();
  vi.clearAllMocks();
  cleanup();
});

describe("TagsFilter advanced search", () => {
  it("searches tags by prefix by default", async () => {
    getDagTagsMock.mockResolvedValue({ tags: ["alpha"], total_entries: 1 });
    render(<FilterBar configs={[tagsConfig]} onFiltersChange={vi.fn()} />, { wrapper });

    await openTagsEditor();
    typeIntoSelect("alp");

    await waitFor(() =>
      expect(getDagTagsMock).toHaveBeenCalledWith(
        expect.objectContaining({ tagNamePrefixPattern: "alp" }),
      ),
    );
    expect(getDagTagsMock).not.toHaveBeenCalledWith(
      expect.objectContaining({ tagNamePattern: "alp" }),
    );
  });

  it("switches to substring matching when the match-anywhere toggle is enabled", async () => {
    getDagTagsMock.mockResolvedValue({ tags: ["alpha"], total_entries: 1 });
    render(<FilterBar configs={[tagsConfig]} onFiltersChange={vi.fn()} />, { wrapper });

    await openTagsEditor();

    const toggle = await screen.findByTestId("advanced-search-toggle");

    expect(toggle.getAttribute("aria-pressed")).toBe("false");
    fireEvent.click(toggle);

    await waitFor(() => expect(localStorage.getItem("advanced_search-tags")).toBe("true"));

    getDagTagsMock.mockClear();
    typeIntoSelect("pha");

    await waitFor(() =>
      expect(getDagTagsMock).toHaveBeenCalledWith(expect.objectContaining({ tagNamePattern: "pha" })),
    );
    expect(getDagTagsMock).not.toHaveBeenCalledWith(
      expect.objectContaining({ tagNamePrefixPattern: "pha" }),
    );
  });

  it("shows a regex indicator on the collapsed pill while substring matching is enabled", () => {
    getDagTagsMock.mockResolvedValue({ tags: ["alpha"], total_entries: 1 });
    localStorage.setItem("advanced_search-tags", "true");
    render(
      <FilterBar configs={[tagsConfig]} initialValues={{ tags: ["alpha"] }} onFiltersChange={vi.fn()} />,
      { wrapper },
    );

    const pill = screen.getByTestId("tags-pill");

    expect(within(pill).getByLabelText("match anywhere")).toBeInTheDocument();
  });

  it("does not render the toggle without supportsAdvancedSearch", async () => {
    const plainConfig: FilterConfig = {
      key: "teams",
      label: "Teams",
      options: [{ label: "alpha", value: "alpha" }],
      type: "multiselect",
    };

    render(<FilterBar configs={[plainConfig]} onFiltersChange={vi.fn()} />, { wrapper });

    fireEvent.click(screen.getByTestId("add-filter-button"));
    fireEvent.click(await screen.findByTestId("add-filter-teams"));

    expect(screen.queryByTestId("advanced-search-toggle")).not.toBeInTheDocument();
  });
});
