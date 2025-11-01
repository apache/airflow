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
import type { Mock } from "vitest";
import { describe, it, expect, vi, beforeEach } from "vitest";
import * as ParamStore from "src/queries/useParamStore";

import { Wrapper } from "src/utils/Wrapper";

import { FieldObject } from "./FieldObject";

// Mock useParamStore
const mockSetParamsDict = vi.fn();

vi.mock("src/queries/useParamStore", () => {
  const mockUseParamStore = vi.fn();

  return {
    // Keep paramPlaceholder first to satisfy object sort rules
    paramPlaceholder: {
      // eslint-disable-next-line unicorn/no-null
      description: null,
      schema: {},
      value: "",
    },
    useParamStore: mockUseParamStore,
  };
});

// Mock JsonEditor
vi.mock("../JsonEditor", () => ({
  JsonEditor: ({ value }: Readonly<{ value: string }>) => (
    <textarea aria-label="json-editor" data-testid="json-editor" readOnly value={value} />
  ),
}));

describe("FieldObject", () => {
  const mockOnUpdate = vi.fn();
  let mockUseParamStore: Mock;

  beforeEach(() => {
    vi.clearAllMocks();
    mockSetParamsDict.mockClear();
    mockOnUpdate.mockClear();

    // Get the mocked function from the statically imported (mocked) module
    mockUseParamStore = (ParamStore.useParamStore as unknown) as Mock;
  });

  it("renders empty object {} when param.value is undefined (using paramPlaceholder)", () => {
    mockUseParamStore.mockReturnValue({
      disabled: false,
      paramsDict: {
        testParam: {
          // eslint-disable-next-line unicorn/no-null
          description: null,
          schema: { type: "object" },
          value: undefined, // Explicitly set to undefined
        },
      },
      setParamsDict: mockSetParamsDict,
    });

    render(<FieldObject name="testParam" onUpdate={mockOnUpdate} />, { wrapper: Wrapper });

    const editor = screen.getByTestId("json-editor");

    // When param.value is undefined, it should default to {} for object type
    // This is the key fix: {} instead of []
    expect(editor).toHaveValue("{}");
  });

  it("renders empty object {} when param.value is null", () => {
    mockUseParamStore.mockReturnValue({
      disabled: false,
      paramsDict: {
        testParam: {
          // eslint-disable-next-line unicorn/no-null
          description: null,
          schema: { type: "object" },
          // eslint-disable-next-line unicorn/no-null
          value: null,
        },
      },
      setParamsDict: mockSetParamsDict,
    });

    render(<FieldObject name="testParam" onUpdate={mockOnUpdate} />, { wrapper: Wrapper });

    const editor = screen.getByTestId("json-editor");

    // When value is null, it should still default to {} for object type
    expect(editor).toHaveValue("{}");
  });

  it("renders empty object {} when param.value is an empty object", () => {
    mockUseParamStore.mockReturnValue({
      disabled: false,
      paramsDict: {
        testParam: {
          description: "Test param",
          schema: { type: "object" },
          value: {},
        },
      },
      setParamsDict: mockSetParamsDict,
    });

    render(<FieldObject name="testParam" onUpdate={mockOnUpdate} />, { wrapper: Wrapper });

    const editor = screen.getByTestId("json-editor");

    expect(editor).toHaveValue("{}");
  });

  it("renders non-empty object correctly", () => {
    const testValue = { key: "value", number: 42 };

    mockUseParamStore.mockReturnValue({
      disabled: false,
      paramsDict: {
        testParam: {
          description: "Test param",
          schema: { type: "object" },
          value: testValue,
        },
      },
      setParamsDict: mockSetParamsDict,
    });

    render(<FieldObject name="testParam" onUpdate={mockOnUpdate} />, { wrapper: Wrapper });

    const editor = screen.getByTestId("json-editor");

    expect(editor).toHaveValue(JSON.stringify(testValue, undefined, 2));
  });
});
