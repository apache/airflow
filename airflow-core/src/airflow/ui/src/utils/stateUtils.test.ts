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

import { sortStateEntries } from "./stateUtils";

describe("sortStateEntries", () => {
  it("returns empty array for null input", () => {
    expect(sortStateEntries(null)).toEqual([]);
  });

  it("returns empty array for undefined input", () => {
    expect(sortStateEntries(undefined)).toEqual([]);
  });

  it("filters out zero-count entries", () => {
    const result = sortStateEntries({ failed: 0, running: 2, success: 0 });

    expect(result).toEqual([["running", 2]]);
  });

  it("sorts entries by state priority (highest priority first)", () => {
    const result = sortStateEntries({
      running: 3,
      scheduled: 4,
      success: 1,
    });

    expect(result).toEqual([
      ["running", 3],
      ["scheduled", 4],
      ["success", 1],
    ]);
  });

  it("places deferred above queued and scheduled", () => {
    const result = sortStateEntries({
      deferred: 2,
      queued: 3,
      scheduled: 1,
    });

    expect(result).toEqual([
      ["deferred", 2],
      ["queued", 3],
      ["scheduled", 1],
    ]);
  });

  it("places failed before running and success", () => {
    const result = sortStateEntries({
      failed: 1,
      running: 2,
      success: 5,
    });

    expect(result).toEqual([
      ["failed", 1],
      ["running", 2],
      ["success", 5],
    ]);
  });

  it("sorts unknown states to the end", () => {
    const result = sortStateEntries({
      running: 2,
      success: 1,
      unknown_state: 3,
    });

    expect(result).toEqual([
      ["running", 2],
      ["success", 1],
      ["unknown_state", 3],
    ]);
  });

  it("returns empty array when all counts are zero", () => {
    expect(sortStateEntries({ failed: 0, running: 0, success: 0 })).toEqual([]);
  });
});
