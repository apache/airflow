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
import { beforeEach, describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { FieldMultiType } from "./FieldMultiType";

type ParamEntry = {
  schema: { type: Array<string> | string | undefined };
  value: unknown;
};
const mockParamsDict: Record<string, ParamEntry> = {};
const mockSetParamsDict = vi.fn();

vi.mock("src/queries/useParamStore", () => ({
  paramPlaceholder: {
    schema: { type: undefined },
    value: null,
  },
  useParamStore: () => ({
    disabled: false,
    paramsDict: mockParamsDict,
    setParamsDict: mockSetParamsDict,
  }),
}));

describe("FieldMultiType", () => {
  beforeEach(() => {
    Object.keys(mockParamsDict).forEach((key) => {
      // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
      delete mockParamsDict[key];
    });
    mockSetParamsDict.mockClear();
  });

  describe("display", () => {
    it("renders a textarea", () => {
      mockParamsDict.test_param = {
        schema: { type: ["string", "object"] },
        value: "nightly",
      };
      render(<FieldMultiType name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });
      expect(screen.getByRole("textbox")).toBeDefined();
    });

    it("displays an object default as pretty-printed JSON", () => {
      const obj = { name: "my_pipeline", retries: 3 };

      mockParamsDict.test_param = {
        schema: { type: ["string", "object"] },
        value: obj,
      };
      render(<FieldMultiType name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });
      expect(screen.getByRole("textbox")).toHaveProperty("value", JSON.stringify(obj, undefined, 2));
    });

    it("displays a string default as-is", () => {
      mockParamsDict.test_param = {
        schema: { type: ["string", "object"] },
        value: "my_pipeline",
      };
      render(<FieldMultiType name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });
      expect(screen.getByRole("textbox")).toHaveProperty("value", "my_pipeline");
    });

    it("displays a number default as a string", () => {
      mockParamsDict.test_param = {
        schema: { type: ["integer", "string"] },
        value: 42,
      };
      render(<FieldMultiType name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });
      expect(screen.getByRole("textbox")).toHaveProperty("value", "42");
    });

    it("displays a boolean default as a string", () => {
      mockParamsDict.test_param = {
        schema: { type: ["boolean", "string"] },
        value: true,
      };
      render(<FieldMultiType name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });
      expect(screen.getByRole("textbox")).toHaveProperty("value", "true");
    });
  });

  describe("type resolution on change", () => {
    it("stores a valid JSON object when schema includes 'object'", () => {
      mockParamsDict.test_param = {
        schema: { type: ["string", "object"] },
        value: {},
      };
      render(<FieldMultiType name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });
      fireEvent.change(screen.getByRole("textbox"), { target: { value: '{"key": "val"}' } });
      expect(mockParamsDict.test_param.value).toEqual({ key: "val" });
    });

    it("stores a plain string when JSON parse fails", () => {
      mockParamsDict.test_param = {
        schema: { type: ["string", "object"] },
        value: {},
      };
      render(<FieldMultiType name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });
      fireEvent.change(screen.getByRole("textbox"), { target: { value: "nightly" } });
      expect(mockParamsDict.test_param.value).toBe("nightly");
    });

    it("stores '45' as a string for type=['string','object'] — number not in schema", () => {
      mockParamsDict.test_param = {
        schema: { type: ["string", "object"] },
        value: {},
      };
      render(<FieldMultiType name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });
      fireEvent.change(screen.getByRole("textbox"), { target: { value: "45" } });
      expect(mockParamsDict.test_param.value).toBe("45");
      expect(typeof mockParamsDict.test_param.value).toBe("string");
    });

    it("stores 45 as a number for type=['integer','string']", () => {
      mockParamsDict.test_param = {
        schema: { type: ["integer", "string"] },
        value: "nightly",
      };
      render(<FieldMultiType name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });
      fireEvent.change(screen.getByRole("textbox"), { target: { value: "45" } });
      expect(mockParamsDict.test_param.value).toBe(45);
      expect(typeof mockParamsDict.test_param.value).toBe("number");
    });

    it("stores a string for type=['integer','string'] when input is non-numeric text", () => {
      mockParamsDict.test_param = {
        schema: { type: ["integer", "string"] },
        value: 0,
      };
      render(<FieldMultiType name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });
      fireEvent.change(screen.getByRole("textbox"), { target: { value: "nightly" } });
      expect(mockParamsDict.test_param.value).toBe("nightly");
    });

    it("stores true as boolean for type=['boolean','string']", () => {
      mockParamsDict.test_param = {
        schema: { type: ["boolean", "string"] },
        value: "pending",
      };
      render(<FieldMultiType name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });
      fireEvent.change(screen.getByRole("textbox"), { target: { value: "true" } });
      expect(mockParamsDict.test_param.value).toBe(true);
    });

    it("stores a string for type=['number','string'] when input is not a number", () => {
      mockParamsDict.test_param = {
        schema: { type: ["number", "string"] },
        value: 0,
      };
      render(<FieldMultiType name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });
      fireEvent.change(screen.getByRole("textbox"), { target: { value: "nightly" } });
      expect(mockParamsDict.test_param.value).toBe("nightly");
    });

    it("stores null on empty input", () => {
      mockParamsDict.test_param = {
        schema: { type: ["string", "object"] },
        value: "something",
      };
      render(<FieldMultiType name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });
      fireEvent.change(screen.getByRole("textbox"), { target: { value: "" } });
      expect(mockParamsDict.test_param.value).toBeNull();
    });

    it("calls onUpdate with the raw input string", () => {
      const onUpdate = vi.fn();

      mockParamsDict.test_param = {
        schema: { type: ["string", "object"] },
        value: {},
      };
      render(<FieldMultiType name="test_param" onUpdate={onUpdate} />, { wrapper: Wrapper });
      fireEvent.change(screen.getByRole("textbox"), { target: { value: "nightly" } });
      expect(onUpdate).toHaveBeenCalledWith("nightly");
    });
  });

  describe("validation errors for schemas without 'string'", () => {
    it("signals error and preserves old value when input doesn't match type=['integer','object']", () => {
      const onUpdate = vi.fn();

      mockParamsDict.test_param = {
        schema: { type: ["integer", "object"] },
        value: 0,
      };
      render(<FieldMultiType name="test_param" onUpdate={onUpdate} />, { wrapper: Wrapper });
      fireEvent.change(screen.getByRole("textbox"), { target: { value: "nightly" } });
      expect(onUpdate).toHaveBeenCalledWith("", expect.stringContaining("integer"));
      expect(mockParamsDict.test_param.value).toBe(0);
    });

    it("accepts a valid integer for type=['integer','object']", () => {
      const onUpdate = vi.fn();

      mockParamsDict.test_param = {
        schema: { type: ["integer", "object"] },
        value: 0,
      };
      render(<FieldMultiType name="test_param" onUpdate={onUpdate} />, { wrapper: Wrapper });
      fireEvent.change(screen.getByRole("textbox"), { target: { value: "42" } });
      expect(onUpdate).toHaveBeenCalledWith("42");
      expect(mockParamsDict.test_param.value).toBe(42);
    });

    it("accepts a valid JSON object for type=['integer','object']", () => {
      const onUpdate = vi.fn();

      mockParamsDict.test_param = {
        schema: { type: ["integer", "object"] },
        value: 0,
      };
      render(<FieldMultiType name="test_param" onUpdate={onUpdate} />, { wrapper: Wrapper });
      fireEvent.change(screen.getByRole("textbox"), { target: { value: '{"key": 1}' } });
      expect(onUpdate).toHaveBeenCalledWith('{"key": 1}');
      expect(mockParamsDict.test_param.value).toEqual({ key: 1 });
    });

    it("signals error for type=['boolean','object'] when input is neither", () => {
      const onUpdate = vi.fn();

      mockParamsDict.test_param = {
        schema: { type: ["boolean", "object"] },
        value: true,
      };
      render(<FieldMultiType name="test_param" onUpdate={onUpdate} />, { wrapper: Wrapper });
      fireEvent.change(screen.getByRole("textbox"), { target: { value: "nightly" } });
      expect(onUpdate).toHaveBeenCalledWith("", expect.stringContaining("boolean"));
      expect(mockParamsDict.test_param.value).toBe(true);
    });

    it("stores '4.5' as a string for type=['integer','string'] — float is not a valid integer", () => {
      mockParamsDict.test_param = {
        schema: { type: ["integer", "string"] },
        value: 0,
      };
      render(<FieldMultiType name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });
      fireEvent.change(screen.getByRole("textbox"), { target: { value: "4.5" } });
      expect(mockParamsDict.test_param.value).toBe("4.5");
      expect(typeof mockParamsDict.test_param.value).toBe("string");
    });
  });
});
