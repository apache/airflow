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

// The whole value of an aria-label, which may span several lines and hold an expression rather
// than one string: aria-label={isCollapsed ? translate("alerts.expandAlerts", ...) : "..."}.
// Counting braces is enough because no aria-label in src puts a brace inside a string, and the
// `${` of a template literal is balanced by its own closing brace.
const readAttributeValue = (source: string, start: number): string => {
  if (source[start] === '"') {
    return source.slice(start, source.indexOf('"', start + 1) + 1);
  }

  let depth = 0;

  for (let index = start; index < source.length; index += 1) {
    if (source[index] === "{") {
      depth += 1;
    } else if (source[index] === "}") {
      depth -= 1;

      if (depth === 0) {
        return source.slice(start, index + 1);
      }
    }
  }

  return source.slice(start);
};

const ARIA_LABEL = /aria-label=(?=[{"])/gu;
const QUOTED = /"[^"]*"|'[^']*'/gu;
const TEMPLATE = /`[^`]*`/gu;
const KEY_CALL = /\b(?:translate|t)\(/gu;
// Only the static halves of a template literal are copy; the interpolated parts are expressions.
const INTERPOLATION = /\$\{[^}]*\}/gu;
// A literal built only out of interpolations and separators says nothing in any language, so
// `${errorTitle} ${String(status)}` is fine while "Show fewer alerts" and `Show ${n} more` are not.
const LETTER = /\p{L}/u;
const SOURCE_PREFIX = /^\.\.\//u;

// Everything between a translate( or t( and its closing paren. Those are keys the locale owns,
// whether the key is one string, assembled from parts or picked by a ternary, so no literal in
// there is copy: translate(showFolders ? "dags:folders.hide" : "dags:folders.show") is fine.
const findKeyCallSpans = (value: string): Array<[number, number]> =>
  [...value.matchAll(KEY_CALL)].map((call) => {
    let depth = 0;

    for (let index = call.index + call[0].length - 1; index < value.length; index += 1) {
      if (value[index] === "(") {
        depth += 1;
      } else if (value[index] === ")") {
        depth -= 1;

        if (depth === 0) {
          return [call.index, index];
        }
      }
    }

    return [call.index, value.length];
  });

const findHardcodedAriaLabels = (file: string, source: string): Array<string> =>
  [...source.matchAll(ARIA_LABEL)].flatMap((attribute) => {
    const value = readAttributeValue(source, attribute.index + attribute[0].length);
    const keySpans = findKeyCallSpans(value);
    const copy = [QUOTED, TEMPLATE]
      .flatMap((pattern) => [...value.matchAll(pattern)])
      .filter((literal) => !keySpans.some(([from, to]) => literal.index > from && literal.index < to))
      .filter((literal) => LETTER.test(literal[0].slice(1, -1).replaceAll(INTERPOLATION, "")));

    return copy.length === 0 ? [] : [`${file.replace(SOURCE_PREFIX, "")}: ${value.replaceAll(/\s+/gu, " ")}`];
  });

describe("aria-label values", () => {
  it("never hardcodes a string an icon-only control is named by", () => {
    const hardcoded = Object.entries(sourceFiles)
      .filter(([file]) => !file.includes(".test."))
      .flatMap(([file, source]) => findHardcodedAriaLabels(file, source));

    expect(hardcoded).toEqual([]);
  });
});
