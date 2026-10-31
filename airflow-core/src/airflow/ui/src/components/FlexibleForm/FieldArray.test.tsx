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
import { describe, it, expect, beforeEach, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { FieldArray } from "./FieldArray";
import { parseArrayElement } from "./parseArrayElement";

type MockParam = { schema: Record<string, unknown>; value: unknown };
const mockParamsDict: Record<string, MockParam> = {};
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

describe("parseArrayElement", () => {
  it.each([
    ["string", "abc", "abc"],
    ["string", " padded ", "padded"],
    ["string", "123", "123"],
    ["number", "1.5", 1.5],
    ["number", "abc", undefined],
    ["integer", "42", 42],
    ["integer", "1.5", undefined],
    ["boolean", "true", true],
    ["boolean", "false", false],
    ["boolean", "yes", undefined],
    [undefined, "1", 1],
    [undefined, "true", true],
    [undefined, "abc", "abc"],
    [undefined, '"1"', "1"],
  ])("items.type=%s: %s -> %s", (itemsType, raw, expected) => {
    expect(parseArrayElement(raw, itemsType)).toStrictEqual(expected);
  });

  it("rejects empty entries", () => {
    expect(parseArrayElement("   ", "string")).toBeUndefined();
  });
});

describe("FieldArray", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    Object.keys(mockParamsDict).forEach((key) => {
      // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
      delete mockParamsDict[key];
    });
  });

  it("renders one chip per element", () => {
    mockParamsDict.test_param = {
      schema: { items: { type: "string" }, type: "array" },
      value: ["schema1", "schema2"],
    };

    render(<FieldArray name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });

    expect(screen.getByText("schema1")).toBeDefined();
    expect(screen.getByText("schema2")).toBeDefined();
  });

  it("adds an element on Enter and keeps the number type", () => {
    mockParamsDict.test_param = {
      schema: { items: { type: "number" }, type: "array" },
      value: [1, 2],
    };

    render(<FieldArray name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });

    const input = screen.getByRole("combobox");

    fireEvent.change(input, { target: { value: "3" } });
    fireEvent.keyDown(input, { key: "Enter" });

    expect(mockParamsDict.test_param.value).toStrictEqual([1, 2, 3]);
    expect(mockSetParamsDict).toHaveBeenCalled();
  });

  it("reports a validation error for an invalid element and does not add it", () => {
    mockParamsDict.test_param = {
      schema: { items: { type: "number" }, type: "array" },
      value: [1],
    };
    const onUpdate = vi.fn();

    render(<FieldArray name="test_param" onUpdate={onUpdate} />, { wrapper: Wrapper });

    const input = screen.getByRole("combobox");

    fireEvent.change(input, { target: { value: "abc" } });
    fireEvent.keyDown(input, { key: "Enter" });

    expect(mockParamsDict.test_param.value).toStrictEqual([1]);
    expect(onUpdate).toHaveBeenCalledWith(undefined, expect.stringContaining("validationErrorArrayElement"));
  });

  it("splits multiline paste into one element per line", () => {
    mockParamsDict.test_param = {
      schema: { items: { type: "string" }, type: "array" },
      value: [],
    };

    render(<FieldArray name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });

    const input = screen.getByRole("combobox");

    fireEvent.paste(input, { clipboardData: { getData: () => "one\ntwo\n\nthree\n" } });

    expect(mockParamsDict.test_param.value).toStrictEqual(["one", "two", "three"]);
  });

  it("preserves element types for untyped arrays", () => {
    mockParamsDict.test_param = {
      schema: { type: "array" },
      value: [1, 2],
    };

    render(<FieldArray name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });

    const input = screen.getByRole("combobox");

    fireEvent.change(input, { target: { value: "3" } });
    fireEvent.keyDown(input, { key: "Enter" });

    expect(mockParamsDict.test_param.value).toStrictEqual([1, 2, 3]);
  });

  it("removes only the clicked chip when two chips have the same value", () => {
    mockParamsDict.test_param = {
      schema: { items: { type: "string" }, type: "array" },
      value: ["dup", "dup", "other"],
    };

    render(<FieldArray name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });

    fireEvent.click(screen.getAllByLabelText("Remove dup")[0] as HTMLElement);

    expect(mockParamsDict.test_param.value).toStrictEqual(["dup", "other"]);
  });

  it("stores null when the last chip is removed", () => {
    mockParamsDict.test_param = {
      schema: { items: { type: "string" }, type: "array" },
      value: ["only"],
    };

    render(<FieldArray name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });

    const input = screen.getByRole("combobox");

    fireEvent.keyDown(input, { key: "Backspace" });

    expect(mockParamsDict.test_param.value).toBeNull();
  });
});
