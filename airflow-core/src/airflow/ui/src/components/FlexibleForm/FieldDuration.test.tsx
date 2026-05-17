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
import { render, screen, fireEvent } from "@testing-library/react";
import { describe, it, expect, beforeEach, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { FieldDuration } from "./FieldDuration";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const mockParamsDict: Record<string, any> = {};
const mockSetParamsDict = vi.fn();

vi.mock("src/queries/useParamStore", () => ({
  paramPlaceholder: {
    schema: {},
    value: null,
  },
  useParamStore: () => ({
    disabled: false,
    paramsDict: mockParamsDict,
    setParamsDict: mockSetParamsDict,
  }),
}));

describe("FieldDuration", () => {
  beforeEach(() => {
    Object.keys(mockParamsDict).forEach((key) => {
      // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
      delete mockParamsDict[key];
    });
    mockSetParamsDict.mockClear();
  });

  it("renders the duration input", () => {
    mockParamsDict.test_duration = { schema: { format: "duration", type: "string" }, value: "" };
    render(<FieldDuration name="test_duration" onUpdate={vi.fn()} />, { wrapper: Wrapper });
    expect(screen.getByRole("textbox")).toBeDefined();
  });

  it("calls onUpdate with value when a valid duration is entered", () => {
    const onUpdate = vi.fn();

    mockParamsDict.test_duration = { schema: { format: "duration", type: "string" }, value: "" };

    render(<FieldDuration name="test_duration" onUpdate={onUpdate} />, { wrapper: Wrapper });

    fireEvent.change(screen.getByRole("textbox"), { target: { value: "PT15M" } });

    expect(onUpdate).toHaveBeenCalledWith("PT15M");
    expect(onUpdate).not.toHaveBeenCalledWith(undefined, expect.any(String));
  });

  it("calls onUpdate with error message when an invalid duration is entered", () => {
    const onUpdate = vi.fn();

    mockParamsDict.test_duration = { schema: { format: "duration", type: "string" }, value: "" };

    render(<FieldDuration name="test_duration" onUpdate={onUpdate} />, { wrapper: Wrapper });

    fireEvent.change(screen.getByRole("textbox"), { target: { value: "garbage" } });

    expect(onUpdate).toHaveBeenCalledWith(undefined, expect.any(String));
  });

  it("normalizes comma decimal separator to dot before calling onUpdate", () => {
    const onUpdate = vi.fn();

    mockParamsDict.test_duration = { schema: { format: "duration", type: "string" }, value: "" };

    render(<FieldDuration name="test_duration" onUpdate={onUpdate} />, { wrapper: Wrapper });

    fireEvent.change(screen.getByRole("textbox"), { target: { value: "PT1,5H" } });

    expect(onUpdate).toHaveBeenCalledWith("PT1.5H");
  });

  it("calls onUpdate with empty string when field is cleared", () => {
    const onUpdate = vi.fn();

    mockParamsDict.test_duration = { schema: { format: "duration", type: "string" }, value: "PT1H" };

    render(<FieldDuration name="test_duration" onUpdate={onUpdate} />, { wrapper: Wrapper });

    fireEvent.change(screen.getByRole("textbox"), { target: { value: "" } });

    expect(onUpdate).toHaveBeenCalledWith("");
  });
});
