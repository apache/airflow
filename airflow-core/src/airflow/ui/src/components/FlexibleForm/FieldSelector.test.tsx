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
import { render } from "@testing-library/react";
import { describe, it, expect, beforeEach, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { FieldSelector } from "./FieldSelector";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const mockParamsDict: Record<string, any> = {};
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const mockInitialParamDict: Record<string, any> = {};

vi.mock("src/queries/useParamStore", () => ({
  paramPlaceholder: {
    description: null,
    schema: {},
    value: "",
  },
  useParamStore: () => ({
    disabled: false,
    initialParamDict: mockInitialParamDict,
    paramsDict: mockParamsDict,
    setParamsDict: vi.fn(),
  }),
}));

let mockSensitiveFieldNames: Array<string> | undefined = [];

vi.mock("src/queries/useConfig", () => ({
  useConfig: () => mockSensitiveFieldNames,
}));

// Match on the name attribute — free-form extra keys contain dots and dashes, which are not
// valid in a `#id` selector.
const getInputByName = (name: string) => document.querySelector<HTMLInputElement>(`[name="element_${name}"]`);

const clearDict = (dict: Record<string, unknown>) => {
  Object.keys(dict).forEach((key) => {
    // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
    delete dict[key];
  });
};

const renderField = (name: string) =>
  render(<FieldSelector name={name} onUpdate={vi.fn()} />, { wrapper: Wrapper });

describe("FieldSelector", () => {
  beforeEach(() => {
    mockSensitiveFieldNames = ["access_key", "password", "private_key"];
    clearDict(mockParamsDict);
    clearDict(mockInitialParamDict);
  });

  it("masks a free-form field whose name is sensitive", () => {
    mockParamsDict.private_key = { schema: {}, value: "-----BEGIN..." };

    renderField("private_key");

    expect(getInputByName("private_key")?.type).toBe("password");
  });

  it("masks a sensitive free-form field that has no value yet", () => {
    mockParamsDict.private_key = { schema: {}, value: null };

    renderField("private_key");

    expect(getInputByName("private_key")?.type).toBe("password");
  });

  it("masks a sensitive name that needs normalization", () => {
    mockParamsDict["spark.hadoop.fs.s3a.Access-Key"] = { schema: {}, value: "AKIA" };

    renderField("spark.hadoop.fs.s3a.Access-Key");

    expect(getInputByName("spark.hadoop.fs.s3a.Access-Key")?.type).toBe("password");
  });

  it("leaves a non-sensitive free-form field as plain text", () => {
    mockParamsDict["spark.executor.memory"] = { schema: {}, value: "2g" };

    renderField("spark.executor.memory");

    expect(getInputByName("spark.executor.memory")?.type).toBe("text");
  });

  it("leaves sensitive names as plain text when masking is disabled", () => {
    mockSensitiveFieldNames = [];
    mockParamsDict.private_key = { schema: {}, value: "-----BEGIN..." };

    renderField("private_key");

    expect(getInputByName("private_key")?.type).toBe("text");
  });

  it("does not turn a non-string sensitive field into a password input", () => {
    mockInitialParamDict.use_private_key = { description: null, schema: { type: "boolean" }, value: false };
    mockParamsDict.use_private_key = { schema: { type: "boolean" }, value: false };

    renderField("use_private_key");

    expect(getInputByName("use_private_key")?.type).toBe("checkbox");
  });

  it("still masks a declared password-format field", () => {
    mockInitialParamDict.token_secret = {
      description: null,
      schema: { format: "password", type: ["string", "null"] },
      value: null,
    };
    mockParamsDict.token_secret = { schema: { format: "password", type: ["string", "null"] }, value: null };
    mockSensitiveFieldNames = [];

    renderField("token_secret");

    expect(getInputByName("token_secret")?.type).toBe("password");
  });
});
