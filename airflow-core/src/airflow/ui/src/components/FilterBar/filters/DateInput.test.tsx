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

import { DateInput } from "./DateInput";

const noop = () => undefined;
const noopInputChange = () => noop;

describe("DateInput", () => {
  beforeEach(() => {
    i18n.addResourceBundle(
      "en",
      "common",
      { filters: { clearInput: "Empty the {{label}} box" } },
      true,
      true,
    );
  });

  it("takes the name of its clear button from the locale", () => {
    render(
      <DateInput
        field="start"
        getBorderColor={() => "gray"}
        getFieldError={noop}
        handleInputChange={noopInputChange}
        inputType="date"
        inputValue="2026-01-01"
        label="From"
        onClear={noop}
        placeholder="YYYY-MM-DD"
      />,
      { wrapper: Wrapper },
    );

    // All four inputs of a date range render this button, so the name has to say which one it
    // clears. The label the caller already translated for the visible text is that name.
    expect(screen.getByRole("button", { name: "Empty the From box" })).toBeInTheDocument();
  });
});
