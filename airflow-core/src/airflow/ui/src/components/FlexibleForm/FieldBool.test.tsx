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
import { describe, expect, it, vi } from "vitest";

import type { SwitchProps } from "src/components/ui";
import type { ParamsSpec } from "src/queries/useDagParams";

import { FieldBool } from "./FieldBool";

const mockSwitchProps = vi.hoisted(() => ({ current: undefined as SwitchProps | undefined }));
const mockParamsDict: ParamsSpec = {
  run_tests: {
    description: null,
    schema: {
      const: undefined,
      description_md: undefined,
      enum: undefined,
      examples: undefined,
      format: undefined,
      items: undefined,
      maximum: undefined,
      maxLength: undefined,
      minimum: undefined,
      minLength: undefined,
      section: undefined,
      title: "Run tests",
      type: "boolean",
      values_display: undefined,
    },
    value: true,
  },
};

vi.mock("src/components/ui", () => ({
  Switch: (props: SwitchProps) => {
    mockSwitchProps.current = props;

    return <div />;
  },
}));

vi.mock("src/queries/useParamStore", () => ({
  paramPlaceholder: {
    description: null,
    schema: {},
    value: null,
  },
  useParamStore: () => ({
    disabled: false,
    paramsDict: mockParamsDict,
    setParamsDict: vi.fn(),
  }),
}));

describe("FieldBool", () => {
  it("right-aligns the toggle within the full control column", () => {
    render(<FieldBool name="run_tests" onUpdate={vi.fn()} />);

    expect(mockSwitchProps.current).toMatchObject({
      justifyContent: "flex-end",
      width: "full",
    });
  });
});
