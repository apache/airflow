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

import { getHighlightColor } from "./utils";

describe("getHighlightColor", () => {
  it("returns orange.emphasized for the current search match", () => {
    expect(
      getHighlightColor({
        currentMatchIndex: 3,
        hash: "",
        index: 3,
        searchMatchIndices: new Set([1, 3, 5]),
      }),
    ).toBe("orange.emphasized");
  });

  it("returns yellow.emphasized for a non-current search match", () => {
    expect(
      getHighlightColor({
        currentMatchIndex: 1,
        hash: "",
        index: 3,
        searchMatchIndices: new Set([1, 3, 5]),
      }),
    ).toBe("yellow.emphasized");
  });

  it("returns brand.emphasized for the URL-hash-linked line when no search is active", () => {
    expect(
      getHighlightColor({
        hash: "5",
        index: 4, // hash "5" maps to index 4 (1-based to 0-based)
        searchMatchIndices: undefined,
      }),
    ).toBe("brand.emphasized");
  });

  it("returns transparent when no condition matches", () => {
    expect(
      getHighlightColor({
        hash: "",
        index: 2,
        searchMatchIndices: undefined,
      }),
    ).toBe("transparent");
  });

  it("returns transparent when search is active but line is not a match", () => {
    expect(
      getHighlightColor({
        currentMatchIndex: 0,
        hash: "",
        index: 7,
        searchMatchIndices: new Set([0, 2]),
      }),
    ).toBe("transparent");
  });

  it("current match takes priority over hash highlight on same line", () => {
    // Line index 4 is both the current match and the hash-linked line (hash "5")
    expect(
      getHighlightColor({
        currentMatchIndex: 4,
        hash: "5",
        index: 4,
        searchMatchIndices: new Set([4]),
      }),
    ).toBe("orange.emphasized");
  });

  it("search match takes priority over hash highlight when not the current match", () => {
    // Line 4 is a match but not the current one; hash also points to it
    expect(
      getHighlightColor({
        currentMatchIndex: 0,
        hash: "5",
        index: 4,
        searchMatchIndices: new Set([0, 4]),
      }),
    ).toBe("yellow.emphasized");
  });

  it("returns transparent when searchMatchIndices is an empty Set", () => {
    expect(
      getHighlightColor({
        currentMatchIndex: undefined,
        hash: "",
        index: 0,
        searchMatchIndices: new Set(),
      }),
    ).toBe("transparent");
  });
});
