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

import { getJsonPreviewEntries } from "./jsonPreview";

describe("getJsonPreviewEntries", () => {
  it("summarises primitive object values", () => {
    expect(
      getJsonPreviewEntries({ empty: "", flag: false, missing: null, name: "prod", score: 0.99 }),
    ).toStrictEqual([
      { id: "empty", isComplex: false, label: "empty", value: '""' },
      { id: "flag", isComplex: false, label: "flag", value: "false" },
      { id: "missing", isComplex: false, label: "missing", value: "null" },
      { id: "name", isComplex: false, label: "name", value: "prod" },
      { id: "score", isComplex: false, label: "score", value: "0.99" },
    ]);
  });

  it("hints at nested values instead of inlining them", () => {
    expect(
      getJsonPreviewEntries({ items: [1, 2], nested: { deep: 1 }, noItems: [], noKeys: {} }),
    ).toStrictEqual([
      { id: "items", isComplex: true, label: "items", value: "[…]" },
      { id: "nested", isComplex: true, label: "nested", value: "{…}" },
      { id: "noItems", isComplex: true, label: "noItems", value: "[]" },
      { id: "noKeys", isComplex: true, label: "noKeys", value: "{}" },
    ]);
  });

  it("labels nothing for arrays of primitives", () => {
    expect(getJsonPreviewEntries(["a", 2])).toStrictEqual([
      { id: "0", isComplex: false, value: "a" },
      { id: "1", isComplex: false, value: "2" },
    ]);
  });

  it.each([
    ["an array of objects", [{ id: 1 }, { id: 2 }]],
    ["an array of arrays", [[1], [2]]],
  ])("summarises %s as a single count", (_label, content) => {
    expect(getJsonPreviewEntries(content)).toStrictEqual([
      { id: "items", isComplex: true, itemCount: 2, value: "[…]" },
    ]);
  });

  it.each([
    ["an empty object", {}],
    ["an empty array", []],
  ])("has no preview for %s", (_label, content) => {
    expect(getJsonPreviewEntries(content)).toBeUndefined();
  });
});
