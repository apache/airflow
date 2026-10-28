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
import { describe, it, expect } from "vitest";

import { hasDagRunConfig } from "./hasDagRunConfig";

describe("hasDagRunConfig", () => {
  it.each([
    { description: "undefined", expected: false, input: undefined },
    { description: "null", expected: false, input: null },
    { description: "an empty object", expected: false, input: {} },
    { description: "a non-empty object", expected: true, input: { country: "FR" } },
    { description: "an object with several keys", expected: true, input: { batch: 42, env: "prod" } },
  ])("returns $expected for $description", ({ expected, input }) => {
    expect(hasDagRunConfig(input)).toBe(expected);
  });
});
