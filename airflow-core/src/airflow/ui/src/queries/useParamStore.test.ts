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
import { describe, expect, it } from "vitest";

import type { ParamsSpec } from "src/queries/useDagParams";

import { paramsDictWithConfValues } from "./useParamStore";

const schema = (type: Array<string> | string, title: string) => ({
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

describe("paramsDictWithConfValues", () => {
  it("hydrates provider extra fields from existing raw JSON values", () => {
    const snowflakeExtraFields: ParamsSpec = {
      account: {
        description: null,
        schema: schema(["string", "null"], "Account"),
        value: undefined,
      },
      insecure_mode: {
        description: "Turns off OCSP certificate checks",
        schema: schema(["boolean", "null"], "Insecure Mode"),
        value: false,
      },
    };

    const paramsDict = paramsDictWithConfValues(snowflakeExtraFields, '{"account":"1234"}');

    expect(paramsDict.account?.value).toBe("1234");
    expect(paramsDict.insecure_mode?.value).toBe(false);
    expect(Object.keys(paramsDict)).toEqual(["account", "insecure_mode"]);
  });

  it("keeps raw JSON keys that are not declared by the provider schema", () => {
    const paramsDict = paramsDictWithConfValues({}, '{"account":"1234"}');

    expect(paramsDict.account?.value).toBe("1234");
  });
});
