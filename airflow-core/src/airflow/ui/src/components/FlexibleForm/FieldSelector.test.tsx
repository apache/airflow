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

describe("FieldSelector enum routing", () => {
  beforeEach(() => {
    Object.keys(mockParamsDict).forEach((key) => {
      // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
      delete mockParamsDict[key];
    });
  });

  it("renders a dropdown for an object param with enum choices", () => {
    const envs = [
      { name: "dev", replicas: 1 },
      { name: "prod", replicas: 5 },
    ];

    mockParamsDict.test_param = {
      schema: { enum: envs, type: "object" },
      value: envs[0],
    };

    render(<FieldSelector name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });

    expect(screen.getByRole("combobox")).toBeDefined();
  });

  it("renders the JSON editor for an object param without enum", () => {
    mockParamsDict.test_param = {
      schema: { type: "object" },
      value: { name: "dev" },
    };

    render(<FieldSelector name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });

    expect(screen.queryByRole("combobox")).toBeNull();
  });
});
