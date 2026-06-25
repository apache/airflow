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
import type { languages } from "monaco-editor";

// Escaped f-string braces: `{{` / `}}` are literal `{` / `}`, not interpolation.
// Matching two braces wins over the single-brace interpolation rule, and a single
// `{` still falls through to interpolation, so these can sit at the front of a
// single-line state or before the interpolation rule in a triple state.
const ESCAPED_BRACE_RULES: Array<languages.IMonarchLanguageRule> = [
  [/\{\{/u, "string"],
  [/\}\}/u, "string"],
];

/**
 * Monaco's bundled Python Monarch grammar mishandles f-strings two ways:
 *
 * 1. Triple-quoted f-strings: `f"""..."""` is routed to the single-line f-string
 *    state whose first rule does `@popall` at end of line, so a multi-line
 *    f-string loses tokenizer sync after its first line: the string leaks past
 *    its closing `"""` (coloring following code as string) or terminates early.
 * 2. Escaped braces: `{{` / `}}` are mis-parsed as interpolation rather than as
 *    literal string content.
 *
 * See https://github.com/apache/airflow/issues/67986.
 *
 * This patch:
 * - rewrites the `strings` state so triple quotes route to two newly added
 *   multi-line states (`fStringBodyTriple` / `fDblStringBodyTriple`); and
 * - prepends escaped-brace rules to the single-line `fStringBody` /
 *   `fDblStringBody` states.
 *
 * Every other state is left untouched. The triple states stay inside the string
 * across line breaks, treat escaped `{{` / `}}` as literal string content, color
 * `{...}` interpolations via the existing `fStringDetail` state, and exit only on
 * the matching triple quote. Returns a new grammar object; `language` is never
 * mutated.
 *
 * Caveats:
 * - This reaches into monaco's bundled basic-language grammar and references its
 *   internal state names (`fStringDetail`, `fStringBody`, `fDblStringBody`).
 *   Verified against monaco-editor 0.52.2; recheck on monaco upgrades (the
 *   tokenizer test in this folder is the guard).
 * - Raw f-strings (`rf"""` / `fr"""`) are out of scope and remain unhandled, as
 *   they already were in the bundled grammar.
 */
export const patchPythonFStrings = (language: languages.IMonarchLanguage): languages.IMonarchLanguage => {
  const tokenizer: languages.IMonarchLanguage["tokenizer"] = {
    ...language.tokenizer,
    fDblStringBody: [...ESCAPED_BRACE_RULES, ...(language.tokenizer.fDblStringBody ?? [])],
    fDblStringBodyTriple: [
      [/"""/u, "string.escape", "@popall"],
      ...ESCAPED_BRACE_RULES,
      [/\{[^!':=}]+/u, "identifier", "@fStringDetail"],
      [/\\./u, "string"],
      [/\\$/u, "string"],
      [/[^"\\{}]+/u, "string"],
      [/["{}]/u, "string"],
    ],
    fStringBody: [...ESCAPED_BRACE_RULES, ...(language.tokenizer.fStringBody ?? [])],
    fStringBodyTriple: [
      [/'''/u, "string.escape", "@popall"],
      ...ESCAPED_BRACE_RULES,
      [/\{[^!':=}]+/u, "identifier", "@fStringDetail"],
      [/\\./u, "string"],
      [/\\$/u, "string"],
      [/[^'\\{}]+/u, "string"],
      [/['{}]/u, "string"],
    ],
    // Triple-quote rules must precede the single-quote rules so `f"""` is not
    // consumed as `f"` + `""`. `[Ff]` covers both `f"""` and `F"""`.
    strings: [
      [/'$/u, "string.escape", "@popall"],
      [/[Ff]'''/u, "string.escape", "@fStringBodyTriple"],
      [/[Ff]'/u, "string.escape", "@fStringBody"],
      [/'/u, "string.escape", "@stringBody"],
      [/"$/u, "string.escape", "@popall"],
      [/[Ff]"""/u, "string.escape", "@fDblStringBodyTriple"],
      [/[Ff]"/u, "string.escape", "@fDblStringBody"],
      [/"/u, "string.escape", "@dblStringBody"],
    ],
  };

  return { ...language, tokenizer };
};
