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

import { formatNumber } from "./formatNumber";

describe("formatNumber", () => {
  it.each([
    ["en", "1,234"],
    ["de", "1.234"],
  ])("groups digits for %s", (locale, expected) => {
    expect(formatNumber(1234, locale)).toBe(expected);
  });

  it("falls back to the default locale instead of throwing on a malformed tag", () => {
    // Plugins can contribute a UI language served via /static/i18n/languages.json without BCP-47
    // validation (e.g. "pt_BR"), which Intl.NumberFormat rejects with a RangeError.
    expect(formatNumber(1234, "pt_BR")).toBe("1,234");
  });
});
