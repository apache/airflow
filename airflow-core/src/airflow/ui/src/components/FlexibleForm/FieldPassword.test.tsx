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
import { fireEvent, render } from "@testing-library/react";
import { describe, it, expect, beforeEach, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { FieldPassword } from "./FieldPassword";

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

const getInputByName = (name: string) =>
  document.querySelector<HTMLInputElement>(`#element_${name}`) as HTMLInputElement;

describe("FieldPassword", () => {
  beforeEach(() => {
    mockSetParamsDict.mockClear();
    Object.keys(mockParamsDict).forEach((key) => {
      // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
      delete mockParamsDict[key];
    });
  });

  it("renders a masked input for a password-format field", () => {
    mockParamsDict.private_key = {
      schema: { format: "password", type: "string" },
      value: null,
    };

    render(<FieldPassword name="private_key" onUpdate={vi.fn()} />, { wrapper: Wrapper });

    expect(getInputByName("private_key").type).toBe("password");
  });

  it("stores the typed value in the param store", () => {
    mockParamsDict.private_key = {
      schema: { format: "password", type: "string" },
      value: null,
    };
    const onUpdate = vi.fn();

    render(<FieldPassword name="private_key" onUpdate={onUpdate} />, { wrapper: Wrapper });

    fireEvent.change(getInputByName("private_key"), { target: { value: "s3cret" } });

    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    expect(mockParamsDict.private_key.value).toBe("s3cret");
    expect(onUpdate).toHaveBeenLastCalledWith("s3cret");
  });

  it("clears the param to null when the input is emptied", () => {
    mockParamsDict.private_key = {
      schema: { format: "password", type: "string" },
      value: "s3cret",
    };
    const onUpdate = vi.fn();

    render(<FieldPassword name="private_key" onUpdate={onUpdate} />, { wrapper: Wrapper });

    fireEvent.change(getInputByName("private_key"), { target: { value: "" } });

    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    expect(mockParamsDict.private_key.value).toBeNull();
    expect(onUpdate).toHaveBeenLastCalledWith("");
  });
  it("toggles between masked and plain text when the button is clicked", () => {
    mockParamsDict.private_key = {
      schema: { format: "password", type: "string" },
      value: "s3cret",
    };

    render(<FieldPassword name="private_key" onUpdate={vi.fn()} />, { wrapper: Wrapper });

    const input = getInputByName("private_key");
    const toggle = document.querySelector("button") as HTMLButtonElement;

    expect(input.type).toBe("password");

    fireEvent.click(toggle);
    expect(input.type).toBe("text");

    fireEvent.click(toggle);
    expect(input.type).toBe("password");
  });
});
