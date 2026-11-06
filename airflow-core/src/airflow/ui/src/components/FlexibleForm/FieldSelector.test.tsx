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
import { render, screen } from "@testing-library/react";
import { describe, it, expect, beforeEach, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { FieldSelector } from "./FieldSelector";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const mockParamsDict: Record<string, any> = {};

vi.mock("src/queries/useParamStore", () => ({
  paramPlaceholder: {
    schema: {},
    value: null,
  },
  useParamStore: () => ({
    disabled: false,
    initialParamDict: mockParamsDict,
    paramsDict: mockParamsDict,
    setParamsDict: vi.fn(),
  }),
}));

describe("FieldSelector array routing", () => {
  beforeEach(() => {
    Object.keys(mockParamsDict).forEach((key) => {
      // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
      delete mockParamsDict[key];
    });
  });

  it.each([
    ["string", ["a"]],
    ["number", [1]],
    ["integer", [1]],
    ["boolean", [true]],
  ])("renders the chips editor for items.type=%s", (itemsType, value) => {
    mockParamsDict.test_param = {
      schema: { items: { type: itemsType }, type: "array" },
      value,
    };

    render(<FieldSelector name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });

    expect(screen.getByRole("combobox")).toBeDefined();
  });

  it("renders the chips editor for untyped arrays", () => {
    mockParamsDict.test_param = {
      schema: { type: "array" },
      value: [1, 2, 3],
    };

    render(<FieldSelector name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });

    expect(screen.getByRole("combobox")).toBeDefined();
  });

  it("renders the JSON editor for object arrays", () => {
    mockParamsDict.test_param = {
      schema: { items: { type: "object" }, type: "array" },
      value: [{ key: "value" }],
    };

    render(<FieldSelector name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });

    expect(screen.queryByRole("combobox")).toBeNull();
  });
});
