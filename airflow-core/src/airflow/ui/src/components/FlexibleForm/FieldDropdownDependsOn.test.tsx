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
import { act, fireEvent, render, renderHook, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { ParamSchema, ParamsSpec } from "src/queries/useDagParams";
import { paramPlaceholder, useParamStore } from "src/queries/useParamStore";
import { Wrapper } from "src/utils/Wrapper";

import { FieldDropdown } from "./FieldDropdown";
import { FieldSelector } from "./FieldSelector";

const param = (value: unknown, schema: Partial<ParamSchema> = {}) => ({
  description: null,
  schema: { ...paramPlaceholder.schema, ...schema },
  value,
});

const cityMap = { de: ["berlin", "munich"], fr: ["paris"] };

const setUp = (namespace: string, params: ParamsSpec) => {
  const { result } = renderHook(() => useParamStore(namespace));

  act(() => {
    result.current.setInitialParamDict(params);
    result.current.setParamsDict(params);
  });

  return result;
};

const openMenu = (name: string) => {
  const input = document.querySelector<HTMLInputElement>(`#element_${name} input`) as HTMLInputElement;

  fireEvent.keyDown(input, { key: "ArrowDown" });
};

describe("FieldDropdown with depends_on", () => {
  it("renders a dropdown with the options for the current parent value", () => {
    setUp("options", {
      city: param(null, { depends_on: "country", options_map: cityMap, type: ["null", "string"] }),
      country: param("de", { enum: ["de", "fr"], type: "string" }),
    });

    render(<FieldSelector name="city" namespace="options" onUpdate={vi.fn()} />, { wrapper: Wrapper });
    openMenu("city");

    expect(screen.getByText("berlin")).toBeDefined();
    expect(screen.getByText("munich")).toBeDefined();
    expect(screen.queryByText("paris")).toBeNull();
  });

  it("clears the value when the parent change removes it from the options", () => {
    const store = setUp("reset", {
      city: param("berlin", { depends_on: "country", options_map: cityMap, type: ["null", "string"] }),
      country: param("de", { enum: ["de", "fr"], type: "string" }),
    });
    const onUpdate = vi.fn();

    render(<FieldDropdown name="city" namespace="reset" onUpdate={onUpdate} />, { wrapper: Wrapper });

    expect(store.current.paramsDict.city?.value).toBe("berlin");
    expect(onUpdate).not.toHaveBeenCalled();

    act(() =>
      store.current.setParamsDict({
        ...store.current.paramsDict,
        country: param("fr", { enum: ["de", "fr"], type: "string" }),
      }),
    );

    expect(store.current.paramsDict.city?.value).toBeNull();
    expect(onUpdate).toHaveBeenCalledWith("");
  });

  it("clears a chain of dependent params", () => {
    const store = setUp("chain", {
      city: param("berlin", { depends_on: "country", options_map: cityMap, type: ["null", "string"] }),
      country: param("de", { enum: ["de", "fr"], type: "string" }),
      district: param("mitte", {
        depends_on: "city",
        options_map: { berlin: ["mitte"], paris: ["marais"] },
        type: ["null", "string"],
      }),
    });

    render(
      <>
        <FieldDropdown name="city" namespace="chain" onUpdate={vi.fn()} />
        <FieldDropdown name="district" namespace="chain" onUpdate={vi.fn()} />
      </>,
      { wrapper: Wrapper },
    );

    act(() =>
      store.current.setParamsDict({
        ...store.current.paramsDict,
        country: param("fr", { enum: ["de", "fr"], type: "string" }),
      }),
    );

    expect(store.current.paramsDict.city?.value).toBeNull();
    expect(store.current.paramsDict.district?.value).toBeNull();
  });

  it("matches non-string parent values as Python str() keys", () => {
    setUp("keys", {
      flag: param(true, { type: "boolean" }),
      item: param(null, {
        depends_on: "flag",
        options_map: { False: ["off"], True: ["on"] },
        type: ["null", "string"],
      }),
    });

    render(<FieldDropdown name="item" namespace="keys" onUpdate={vi.fn()} />, { wrapper: Wrapper });
    openMenu("item");

    expect(screen.getByText("on")).toBeDefined();
    expect(screen.queryByText("off")).toBeNull();
  });
});
