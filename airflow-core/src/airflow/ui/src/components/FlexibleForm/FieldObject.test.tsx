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
import { describe, it, expect, beforeEach, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { FieldObject } from "./FieldObject";

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

vi.mock("src/components/MonacoEditor", () => ({
  default: ({
    onChange,
    value,
  }: {
    readonly onChange?: (value: string | undefined) => void;
    readonly value?: string;
  }) => (
    <textarea aria-label="JSON editor" onChange={(event) => onChange?.(event.target.value)} value={value} />
  ),
}));

vi.mock("src/context/colorMode", () => ({
  useMonacoTheme: () => ({ beforeMount: vi.fn(), theme: "airflow-light" }),
}));

describe("FieldObject", () => {
  beforeEach(() => {
    Object.keys(mockParamsDict).forEach((key) => {
      // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
      delete mockParamsDict[key];
    });
  });

  it("renders an empty object for an object param with no value", () => {
    mockParamsDict.test_param = {
      schema: { type: ["object", "null"] },
      value: null,
    };

    render(<FieldObject name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });

    expect(screen.getByLabelText("JSON editor")).toHaveValue("{}");
  });

  it("renders the existing value for an object param that has one", () => {
    mockParamsDict.test_param = {
      schema: { type: "object" },
      value: { key: "value" },
    };

    render(<FieldObject name="test_param" onUpdate={vi.fn()} />, { wrapper: Wrapper });

    expect(screen.getByLabelText("JSON editor")).toHaveValue(JSON.stringify({ key: "value" }, undefined, 2));
  });
});
