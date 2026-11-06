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

// `i18next/no-literal-string` runs in its default `jsx-text-only` mode, which checks JSX text and
// no attributes, so an aria-label is the one piece of user-facing copy lint cannot see. Widening
// the rule to attributes reports 27 strings that are fine, mostly translation keys inside the
// `translate(...)` call it is meant to encourage, so the narrow check lives here instead.
const sourceFiles: Record<string, string> = import.meta.glob("../**/*.tsx", {
  eager: true,
  import: "default",
  query: "?raw",
});

// Only a fully quoted value, with or without braces: aria-label="Next page" or
// aria-label={"Next page"}. A template literal that interpolates a value needs a key with
// its own interpolation, so those are left for a follow-up rather than exempted here.
const LITERAL_ARIA_LABEL = /aria-label=\{?"(?<label>[^"]*)"/gu;
const SOURCE_PREFIX = /^\.\.\//u;

const findLiteralAriaLabels = (file: string, source: string): Array<string> =>
  [...source.matchAll(LITERAL_ARIA_LABEL)].map(
    (match) => `${file.replace(SOURCE_PREFIX, "")}: aria-label="${match.groups?.label ?? ""}"`,
  );

describe("aria-label values", () => {
  it("never hardcodes a string an icon-only control is named by", () => {
    const hardcoded = Object.entries(sourceFiles)
      .filter(([file]) => !file.includes(".test."))
      .flatMap(([file, source]) => findLiteralAriaLabels(file, source));

    expect(hardcoded).toEqual([]);
  });
});
