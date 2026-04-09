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

import { FieldDropdown } from "./FieldDropdown";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const mockParamsDict: Record<string, any> = {};
const mockSetParamsDict = vi.fn();

vi.mock("src/queries/useParamStore", () => ({
  paramPlaceholder: {
    schema: {},
    // eslint-disable-next-line unicorn/no-null
    value: null,
  },
  useParamStore: () => ({
    disabled: false,
    paramsDict: mockParamsDict,
    setParamsDict: mockSetParamsDict,
  }),
}));

describe("FieldDropdown", () => {
  beforeEach(() => {
    Object.keys(mockParamsDict).forEach((key) => {
      // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
      delete mockParamsDict[key];
    });
  });

  it("renders dropdown with null value in enum", () => {
    mockParamsDict.test_param = {
      schema: {
        // eslint-disable-next-line unicorn/no-null
        enum: [1, 2, 3, null],
        type: ["number", "null"],
      },
      // eslint-disable-next-line unicorn/no-null
      value: null,
    };

    render(<FieldDropdown name="test_param" onUpdate={vi.fn()} />, {
      wrapper: Wrapper,
    });

    expect(screen.getByRole("combobox")).toBeDefined();
  });

  it("displays custom label for null value via values_display", () => {
    mockParamsDict.test_param = {
      schema: {
        // eslint-disable-next-line unicorn/no-null
        enum: [1, 2, 3, null],
        type: ["number", "null"],
        values_display: {
          "1": "One",
          "2": "Two",
          "3": "Three",
          null: "None (Optional)",
        },
      },
      value: 2,
    };

    render(<FieldDropdown name="test_param" onUpdate={vi.fn()} />, {
      wrapper: Wrapper,
    });

    expect(screen.getByRole("combobox")).toBeDefined();
  });

  it("handles string enum with null value", () => {
    mockParamsDict.test_param = {
      schema: {
        // eslint-disable-next-line unicorn/no-null
        enum: ["option1", "option2", null],
        type: ["string", "null"],
      },
      value: "option1",
    };

    render(<FieldDropdown name="test_param" onUpdate={vi.fn()} />, {
      wrapper: Wrapper,
    });

    expect(screen.getByRole("combobox")).toBeDefined();
  });

  it("handles enum with only null value", () => {
    mockParamsDict.test_param = {
      schema: {
        // eslint-disable-next-line unicorn/no-null
        enum: [null],
        type: ["null"],
      },
      // eslint-disable-next-line unicorn/no-null
      value: null,
    };

    render(<FieldDropdown name="test_param" onUpdate={vi.fn()} />, {
      wrapper: Wrapper,
    });

    expect(screen.getByRole("combobox")).toBeDefined();
  });

  it("renders when current value is null", () => {
    mockParamsDict.test_param = {
      schema: {
        // eslint-disable-next-line unicorn/no-null
        enum: ["value1", "value2", "value3", null],
        type: ["string", "null"],
      },
      // eslint-disable-next-line unicorn/no-null
      value: null,
    };

    render(<FieldDropdown name="test_param" onUpdate={vi.fn()} />, {
      wrapper: Wrapper,
    });

    expect(screen.getByRole("combobox")).toBeDefined();
  });

  it("preserves numeric type when selecting a number enum value (prevents 400 Bad Request)", () => {
    mockParamsDict.test_param = {
      schema: {
        // eslint-disable-next-line unicorn/no-null
        enum: [1, 2, 3, 4, 5, 6, 7, 8, 9, null],
        type: ["number", "null"],
        values_display: {
          "1": "One",
          "6": "Six",
        },
      },
      // eslint-disable-next-line unicorn/no-null
      value: null,
    };

    render(<FieldDropdown name="test_param" onUpdate={vi.fn()} />, {
      wrapper: Wrapper,
    });

    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    const enumValues = mockParamsDict.test_param.schema.enum as Array<number | string | null>;
    const selectedString = "6";
    const original = enumValues.find((val) => String(val ?? "__null__") === selectedString);

    expect(original).toBe(6);
    expect(typeof original).toBe("number");
  });
});
