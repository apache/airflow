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
import { act, fireEvent, render, renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { ParamSchema } from "src/queries/useDagParams";
import { paramPlaceholder, useParamStore } from "src/queries/useParamStore";
import { Wrapper } from "src/utils/Wrapper";

import { FieldNumber } from "./FieldNumber";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    i18n: { language: "en" },
    // eslint-disable-next-line id-length
    t: (translationKey: string, options?: Record<string, unknown>) =>
      options ? `${translationKey} ${JSON.stringify(options)}` : translationKey,
  }),
}));

const makeSchema = (schema: Partial<ParamSchema>): ParamSchema => ({
  ...paramPlaceholder.schema,
  ...schema,
});

const boundedSchema = makeSchema({ maximum: 600, minimum: 1, type: "integer" });

const setParams = (params: Record<string, { schema: ParamSchema; value: unknown }>) => {
  const { result } = renderHook(() => useParamStore("default"));

  act(() => {
    result.current.setParamsDict(
      Object.fromEntries(
        Object.entries(params).map(([name, param]) => [
          name,
          { description: null, schema: param.schema, value: param.value },
        ]),
      ),
    );
  });

  return result;
};

const getInputByName = (name: string) =>
  document.querySelector<HTMLInputElement>(`input[name="element_${name}"]`) as HTMLInputElement;

const typeValue = (name: string, value: string) => {
  const input = getInputByName(name);

  fireEvent.focus(input);
  fireEvent.input(input, { target: { value } });

  return input;
};

describe("FieldNumber", () => {
  beforeEach(() => {
    const { result } = renderHook(() => useParamStore("default"));

    act(() => {
      result.current.setParamsDict({});
      result.current.setInitialParamDict({});
    });
  });

  it("keeps a value above the schema maximum instead of clamping it", async () => {
    const store = setParams({ hold_seconds: { schema: boundedSchema, value: 180 } });
    const onUpdate = vi.fn();

    render(<FieldNumber name="hold_seconds" onUpdate={onUpdate} />, { wrapper: Wrapper });

    const input = typeValue("hold_seconds", "6666");

    await waitFor(() => expect(store.current.paramsDict.hold_seconds?.value).toBe(6666));

    fireEvent.blur(input);

    await waitFor(() => expect(input).toHaveValue("6666"));
    expect(store.current.paramsDict.hold_seconds?.value).toBe(6666);
  });

  it("keeps a value below the schema minimum instead of clamping it", async () => {
    const store = setParams({ hold_seconds: { schema: boundedSchema, value: 180 } });
    const onUpdate = vi.fn();

    render(<FieldNumber name="hold_seconds" onUpdate={onUpdate} />, { wrapper: Wrapper });

    const input = typeValue("hold_seconds", "-5");

    await waitFor(() => expect(store.current.paramsDict.hold_seconds?.value).toBe(-5));

    fireEvent.blur(input);

    await waitFor(() => expect(input).toHaveValue("-5"));
    expect(store.current.paramsDict.hold_seconds?.value).toBe(-5);
  });

  it("reports a validation error for a value above the schema maximum", async () => {
    setParams({ hold_seconds: { schema: boundedSchema, value: 180 } });
    const onUpdate = vi.fn();

    render(<FieldNumber name="hold_seconds" onUpdate={onUpdate} />, { wrapper: Wrapper });

    typeValue("hold_seconds", "6666");

    await waitFor(() =>
      expect(onUpdate).toHaveBeenLastCalledWith(
        undefined,
        'flexibleForm.validationErrorMaximum {"maximum":600}',
      ),
    );
  });

  it("reports a validation error for a value below the schema minimum", async () => {
    setParams({ hold_seconds: { schema: boundedSchema, value: 180 } });
    const onUpdate = vi.fn();

    render(<FieldNumber name="hold_seconds" onUpdate={onUpdate} />, { wrapper: Wrapper });

    typeValue("hold_seconds", "0");

    await waitFor(() =>
      expect(onUpdate).toHaveBeenLastCalledWith(
        undefined,
        'flexibleForm.validationErrorMinimum {"minimum":1}',
      ),
    );
  });
});
