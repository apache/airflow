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

import { FieldMultiSelect } from "./FieldMultiSelect";

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

describe("FieldMultiSelect", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    Object.keys(mockParamsDict).forEach((key) => {
      // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
      delete mockParamsDict[key];
    });
  });

  it("renders selected object values by their JSON form", () => {
    const envs = [
      { name: "dev", replicas: 1 },
      { name: "prod", replicas: 5 },
    ];

    mockParamsDict.test_param = {
      schema: { enum: envs, type: "array" },
      value: [envs[0]],
    };

    render(<FieldMultiSelect name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });

    expect(screen.getByText(JSON.stringify(envs[0]))).toBeDefined();
  });

  it("stores the original objects when object choices are selected", () => {
    const envs = [
      { name: "dev", replicas: 1 },
      { name: "prod", replicas: 5 },
    ];

    mockParamsDict.test_param = {
      schema: { enum: envs, type: "array" },
      value: null,
    };

    render(<FieldMultiSelect name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });

    fireEvent.keyDown(screen.getByRole("combobox"), { key: "ArrowDown" });
    fireEvent.click(screen.getByText(JSON.stringify(envs[1])));

    expect(mockParamsDict.test_param.value).toStrictEqual([envs[1]]);
  });

  it("preserves numeric type when selecting a number choice", () => {
    mockParamsDict.test_param = {
      schema: { enum: [1, 2, 3], type: "array" },
      value: null,
    };

    render(<FieldMultiSelect name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });

    fireEvent.keyDown(screen.getByRole("combobox"), { key: "ArrowDown" });
    fireEvent.click(screen.getByText("2"));

    expect(mockParamsDict.test_param.value).toStrictEqual([2]);
  });
});
