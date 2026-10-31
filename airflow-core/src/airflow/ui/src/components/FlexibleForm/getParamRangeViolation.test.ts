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

import type { ParamSchema } from "src/queries/useDagParams";
import { paramPlaceholder } from "src/queries/useParamStore";

import { getParamRangeViolation } from "./getParamRangeViolation";

const makeSchema = (schema: Partial<ParamSchema>): ParamSchema => ({
  ...paramPlaceholder.schema,
  ...schema,
});

describe("getParamRangeViolation", () => {
  it("reports a value below the schema minimum", () => {
    const schema = makeSchema({ minimum: 1 });

    expect(getParamRangeViolation(0, schema)).toStrictEqual({ kind: "minimum", minimum: 1 });
  });

  it("reports a value above the schema maximum", () => {
    const schema = makeSchema({ maximum: 600 });

    expect(getParamRangeViolation(6666, schema)).toStrictEqual({ kind: "maximum", maximum: 600 });
  });

  it("accepts a value within the schema bounds", () => {
    const schema = makeSchema({ maximum: 600, minimum: 1 });

    expect(getParamRangeViolation(180, schema)).toBeUndefined();
  });

  it("accepts a value equal to the schema bounds", () => {
    const schema = makeSchema({ maximum: 600, minimum: 1 });

    expect(getParamRangeViolation(1, schema)).toBeUndefined();
    expect(getParamRangeViolation(600, schema)).toBeUndefined();
  });

  it("accepts any value when the schema has no bounds", () => {
    const schema = makeSchema({ type: "integer" });

    expect(getParamRangeViolation(-5, schema)).toBeUndefined();
    expect(getParamRangeViolation(6666, schema)).toBeUndefined();
  });
});
