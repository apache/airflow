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
import { act, renderHook } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import type { ParamsSpec, ParamSpec } from "src/queries/useDagParams";

import { useParamStore } from "./useParamStore";

const baseSchema = {
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
  values_display: undefined,
};

let namespaceId = 0;

const getStore = () => {
  const namespace = `use-param-store-test-${namespaceId}`;

  namespaceId += 1;

  return renderHook(() => useParamStore(namespace));
};

const createParam = (title: string, value: unknown, type: ParamSpec["schema"]["type"]): ParamSpec => ({
  description: null,
  schema: {
    ...baseSchema,
    title,
    type,
  },
  value,
});

const createSnowflakeParams = (): ParamsSpec => ({
  account: createParam("Account", undefined, ["string", "null"]),
  insecure_mode: createParam("Insecure Mode", false, ["boolean", "null"]),
  warehouse: createParam("Warehouse", undefined, ["string", "null"]),
});

const setParamValue = (params: ParamsSpec, key: string, value: unknown) => {
  const param = params[key];

  if (param === undefined) {
    throw new Error(`Missing test param: ${key}`);
  }

  param.value = value;
};

describe("useParamStore", () => {
  it("keeps the default params-to-conf serialization behavior", () => {
    const { result } = getStore();
    const params = createSnowflakeParams();

    setParamValue(params, "account", "1234");

    act(() => result.current.setParamsDict(params));

    expect(JSON.parse(result.current.conf) as Record<string, unknown>).toEqual({
      account: "1234",
      insecure_mode: false,
    });
  });

  it("initializes provider params from raw conf without injecting provider defaults", () => {
    const { result } = getStore();

    act(() => {
      result.current.setMergeParamUpdatesIntoConf(true);
      result.current.setConf('{"account":"1234","unknown":"keep"}');
      result.current.initParamsDictFromConf(createSnowflakeParams());
    });

    expect(result.current.paramsDict.account?.value).toBe("1234");
    expect(result.current.paramsDict.insecure_mode?.value).toBeUndefined();
    expect(result.current.paramsDict.unknown).toBeUndefined();
    expect(JSON.parse(result.current.conf) as Record<string, unknown>).toEqual({
      account: "1234",
      unknown: "keep",
    });
  });

  it("merges changed provider fields into raw conf while preserving unknown keys", () => {
    const { result } = getStore();

    act(() => {
      result.current.setMergeParamUpdatesIntoConf(true);
      result.current.setConf('{"account":"1234","unknown":"keep"}');
      result.current.initParamsDictFromConf(createSnowflakeParams());
    });

    const nextParams = structuredClone(result.current.paramsDict);

    setParamValue(nextParams, "warehouse", "test_warehouse");

    act(() => result.current.setParamsDict(nextParams));

    const parsedConf = JSON.parse(result.current.conf) as Record<string, unknown>;

    expect(parsedConf).toEqual({
      account: "1234",
      unknown: "keep",
      warehouse: "test_warehouse",
    });
    expect(parsedConf).not.toHaveProperty("insecure_mode");
  });

  it("removes cleared provider fields from raw conf without dropping unrelated keys", () => {
    const { result } = getStore();

    act(() => {
      result.current.setMergeParamUpdatesIntoConf(true);
      result.current.setConf('{"account":"1234","unknown":"keep","warehouse":"test_warehouse"}');
      result.current.initParamsDictFromConf(createSnowflakeParams());
    });

    const nextParams = structuredClone(result.current.paramsDict);

    setParamValue(nextParams, "account", null);

    act(() => result.current.setParamsDict(nextParams));

    expect(JSON.parse(result.current.conf) as Record<string, unknown>).toEqual({
      unknown: "keep",
      warehouse: "test_warehouse",
    });
  });
});
