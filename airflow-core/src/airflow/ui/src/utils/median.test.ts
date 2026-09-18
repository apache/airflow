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

import { median } from "./median";

describe("median", () => {
  it("returns 0 for an empty list", () => {
    expect(median([])).toBe(0);
  });

  it("returns the middle value for an odd number of entries", () => {
    expect(median([30, 10, 20])).toBe(20);
  });

  it("averages the two middle values for an even number of entries", () => {
    expect(median([10, 20, 30, 40])).toBe(25);
  });

  it("sorts numerically rather than lexicographically", () => {
    expect(median([9, 10, 100])).toBe(10);
  });

  it("does not mutate the input", () => {
    const values = [30, 10, 20];

    median(values);

    expect(values).toStrictEqual([30, 10, 20]);
  });

  it("stays near the bulk of the runs when one run is stuck", () => {
    expect(median([60, 62, 58, 61, 36_000])).toBe(61);
  });
});
