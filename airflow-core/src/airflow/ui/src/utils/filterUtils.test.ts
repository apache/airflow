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

import { getFilterCount } from "./filterUtils";

describe("getFilterCount", () => {
  it("counts non-undefined values correctly", () => {
    const filters = {
      count: 5,
      enabled: true,
      name: "test",
      nullValue: undefined,
      status: "active",
      undefinedValue: undefined,
    };

    expect(getFilterCount(filters)).toBe(4);
  });

  it("handles array fields correctly", () => {
    const filters = {
      emptyTags: [],
      nullValue: undefined,
      status: "active",
      tags: ["tag1", "tag2"],
    };

    expect(getFilterCount(filters)).toBe(2);
  });

  it("returns 0 for empty filters", () => {
    expect(getFilterCount({})).toBe(0);
  });

  it("returns 0 when all values are undefined/undefined", () => {
    const filters = {
      nullValue: undefined,
      undefinedValue: undefined,
    };

    expect(getFilterCount(filters)).toBe(0);
  });

  it("handles boolean values correctly", () => {
    const filters = {
      disabled: false,
      enabled: true,
      nullValue: undefined,
    };

    expect(getFilterCount(filters)).toBe(2);
  });

  it("handles zero values correctly", () => {
    const filters = {
      amount: 0.0,
      count: 0,
      nullValue: undefined,
    };

    expect(getFilterCount(filters)).toBe(2);
  });

  it("handles empty strings correctly", () => {
    const filters = {
      emptyString: "",
      nonEmptyString: "value",
      nullValue: undefined,
    };

    expect(getFilterCount(filters)).toBe(2);
  });
});

it("handles complex filters pattern", () => {
  const filters = {
    after: "2024-01-01",
    before: ["tag1", "tag2"],
    dagId: "test-dag",
    eventType: "event",
    mapIndex: undefined,
    runId: "test-run",
    taskId: undefined,
    tryNumber: undefined,
    user: "admin",
  };

  expect(getFilterCount(filters)).toBe(6);
});
