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
import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it } from "vitest";

import i18n from "src/i18n/config";
import { Wrapper } from "src/utils/Wrapper";

import { FilterPill } from "./FilterPill";
import type { FilterState } from "./types";

const pausedFilter: FilterState = {
  config: { key: "paused", label: "Paused", type: "boolean" },
  id: "paused",
  value: "true",
};

describe("FilterPill", () => {
  beforeEach(() => {
    i18n.addResourceBundle(
      "en",
      "common",
      { filters: { removeNamedFilter: "Drop the {{label}} filter" } },
      true,
      true,
    );
  });

  it("takes the name of the filter it removes from the locale", () => {
    render(
      <FilterPill
        displayValue="Yes"
        filter={pausedFilter}
        hasValue
        onClick={() => undefined}
        onRemove={() => undefined}
        renderInput={() => null}
      />,
      { wrapper: Wrapper },
    );

    // IconButton spreads the caller's props after its own `aria-label={label}`, so an aria-label
    // passed here wins over the translated one and has to be translated itself.
    expect(screen.getByRole("button", { name: "Drop the Paused filter" })).toBeInTheDocument();
  });
});
