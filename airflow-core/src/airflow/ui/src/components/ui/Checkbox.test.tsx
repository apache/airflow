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
import { describe, expect, it } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import { Checkbox } from "./Checkbox";

// Chakra Checkbox exposes an `ids` prop for addressing its rendered parts in composition.
const checkboxPartIds = {
  control: "checkbox-control",
  label: "checkbox-label",
  root: "checkbox-root",
};

describe("Checkbox", () => {
  it("renders an enabled native checkbox input", () => {
    render(<Checkbox>Enabled checkbox</Checkbox>, { wrapper: BaseWrapper });

    expect(screen.getByRole("checkbox", { name: "Enabled checkbox" })).toBeEnabled();
  });

  it("renders a disabled native checkbox input", () => {
    render(<Checkbox disabled>Disabled checkbox</Checkbox>, { wrapper: BaseWrapper });

    expect(screen.getByRole("checkbox", { name: "Disabled checkbox" })).toBeDisabled();
  });

  it("shows enabled checkbox parts as clickable", () => {
    render(<Checkbox ids={checkboxPartIds}>Enabled checkbox</Checkbox>, { wrapper: BaseWrapper });

    expect(document.querySelector(`#${checkboxPartIds.root}`)).toHaveStyle({ cursor: "pointer" });
    expect(document.querySelector(`#${checkboxPartIds.control}`)).toHaveStyle({ cursor: "pointer" });
    expect(document.querySelector(`#${checkboxPartIds.label}`)).toHaveStyle({ cursor: "pointer" });
  });

  it("shows disabled checkbox parts as unavailable", () => {
    render(
      <Checkbox disabled ids={checkboxPartIds}>
        Disabled checkbox
      </Checkbox>,
      { wrapper: BaseWrapper },
    );

    expect(document.querySelector(`#${checkboxPartIds.root}`)).toHaveStyle({ cursor: "not-allowed" });
    expect(document.querySelector(`#${checkboxPartIds.control}`)).toHaveStyle({ cursor: "not-allowed" });
    expect(document.querySelector(`#${checkboxPartIds.label}`)).toHaveStyle({ cursor: "not-allowed" });
  });
});
