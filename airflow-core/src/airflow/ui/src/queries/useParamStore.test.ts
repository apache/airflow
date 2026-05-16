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

import type { ParamSchema, ParamsSpec } from "src/queries/useDagParams";
import { buildParamsDictWithConfValues, useParamStore } from "src/queries/useParamStore";

const buildSchema = (type: ParamSchema["type"], title: string): ParamSchema => ({
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
  title,
  type,
  values_display: undefined,
});

const snowflakeExtraFields: ParamsSpec = {
  account: {
    description: null,
    schema: buildSchema(["string", "null"], "Account"),
    value: undefined,
  },
  insecure_mode: {
    description: "Turns off OCSP certificate checks",
    schema: buildSchema(["boolean", "null"], "Insecure Mode"),
    value: false,
  },
};

let namespaceId = 0;

const renderParamStore = () => {
  namespaceId += 1;

  return renderHook(() => useParamStore(`use-param-store-${namespaceId}`));
};

const parseConf = (conf: string): Record<string, unknown> => JSON.parse(conf) as Record<string, unknown>;

describe("buildParamsDictWithConfValues", () => {
  it("hydrates provider extra fields from existing raw JSON values", () => {
    const paramsDict = buildParamsDictWithConfValues(snowflakeExtraFields, '{"account":"1234"}');

    expect(paramsDict.account?.value).toBe("1234");
    expect(paramsDict.insecure_mode?.value).toBe(false);
    expect(Object.keys(paramsDict)).toStrictEqual(["account", "insecure_mode"]);
  });

  it("keeps raw JSON keys that are not declared by the provider schema", () => {
    const paramsDict = buildParamsDictWithConfValues(snowflakeExtraFields, '{"warehouse":"raw"}');

    expect(paramsDict.warehouse?.value).toBe("raw");
  });
});

describe("useParamStore", () => {
  it("keeps raw-only JSON keys after form initialization", () => {
    const { result } = renderParamStore();

    act(() => result.current.setConf('{"account":"1234","warehouse":"raw"}'));
    act(() => result.current.initializeParamsDict(snowflakeExtraFields));

    expect(parseConf(result.current.conf)).toStrictEqual({
      account: "1234",
      warehouse: "raw",
    });
    expect(result.current.paramsDict.warehouse?.value).toBe("raw");
  });

  it("does not inject an untouched boolean default into conf", () => {
    const { result } = renderParamStore();

    act(() => result.current.setConf('{"account":"1234"}'));
    act(() => result.current.initializeParamsDict(snowflakeExtraFields));

    expect(parseConf(result.current.conf)).toStrictEqual({ account: "1234" });
  });

  it("serializes an unchanged boolean default after the user touches that field", () => {
    const { result } = renderParamStore();

    act(() => result.current.setConf('{"account":"1234"}'));
    act(() => result.current.initializeParamsDict(snowflakeExtraFields));

    const paramsDict = structuredClone(result.current.paramsDict);
    const insecureModeParam = paramsDict.insecure_mode;

    if (insecureModeParam === undefined) {
      throw new Error("Expected insecure_mode param");
    }

    insecureModeParam.value = false;
    act(() => result.current.setParamsDict(paramsDict, "insecure_mode"));

    expect(parseConf(result.current.conf)).toStrictEqual({
      account: "1234",
      insecure_mode: false,
    });
  });

  it("lets a user-set widget value override a raw JSON key with the same name", () => {
    const { result } = renderParamStore();

    act(() => result.current.setConf('{"account":"1234","warehouse":"raw"}'));
    act(() => result.current.initializeParamsDict(snowflakeExtraFields));

    const paramsDict = structuredClone(result.current.paramsDict);
    const accountParam = paramsDict.account;

    if (accountParam === undefined) {
      throw new Error("Expected account param");
    }

    accountParam.value = "5678";
    act(() => result.current.setParamsDict(paramsDict, "account"));

    expect(parseConf(result.current.conf)).toStrictEqual({
      account: "5678",
      warehouse: "raw",
    });
  });

  it("keeps raw-only JSON keys when a user edits another widget", () => {
    const { result } = renderParamStore();

    act(() => result.current.setConf('{"warehouse":"raw"}'));
    act(() => result.current.initializeParamsDict(snowflakeExtraFields));

    const paramsDict = structuredClone(result.current.paramsDict);
    const accountParam = paramsDict.account;

    if (accountParam === undefined) {
      throw new Error("Expected account param");
    }

    accountParam.value = "1234";
    act(() => result.current.setParamsDict(paramsDict, "account"));

    expect(parseConf(result.current.conf)).toStrictEqual({
      account: "1234",
      warehouse: "raw",
    });
  });
});
