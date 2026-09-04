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
import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ChakraWrapper } from "src/utils/ChakraWrapper";

import type { FilterPluginProps } from "../types";
import { TextSearchFilter } from "./TextSearchFilter";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    i18n: { language: "en" },
    // eslint-disable-next-line id-length
    t: (key: string) => key,
  }),
}));

const defaultProps: FilterPluginProps = {
  filter: {
    config: {
      key: "key",
      label: "Key",
      type: "text" as const,
    },
    id: "test-filter",
    value: undefined,
  },
  onChange: vi.fn(),
  onRemove: vi.fn(),
};

describe("TextSearchFilter", () => {
  it("exposes the actively-editing pill's input via a stable, unique testid", () => {
    render(<TextSearchFilter {...defaultProps} />, { wrapper: ChakraWrapper });

    // Regression guard for #72433: e2e tests locate this input via `filter-pill-input`
    // rather than `page.locator("div").filter({ hasText })`, which matched any ancestor
    // whose descendant text contained the filter label. `getByTestId` throws if more than
    // one match is found, so this also proves the testid stays unique while a pill is
    // being edited.
    expect(screen.getByTestId("filter-pill-input")).toBe(screen.getByRole("textbox"));
  });
});
