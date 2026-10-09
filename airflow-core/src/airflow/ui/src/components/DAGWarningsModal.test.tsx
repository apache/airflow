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

import { countDagWarnings } from "./DAGWarningsModal";

describe("countDagWarnings", () => {
  it.each([
    [undefined, null, 0],
    [3, null, 3],
    [undefined, new Error("boom"), 1],
    [0, new Error("boom"), 1],
    [3, new Error("boom"), 4],
  ])("counts %s warnings and error %s as %s", (warningsTotal, error, expected) => {
    expect(countDagWarnings(warningsTotal, error)).toBe(expected);
  });
});
