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

import { patchPythonFStringEscapedBraces } from "./configureMonaco";

type Rule = [RegExp | string, string, string?];

const interpolationRule: Rule = [/\{[^\}':!=]+/, "identifier", "@fStringDetail"];

const buildLanguage = () => ({
  tokenizer: {
    fDblStringBody: [
      [/[^"]+/, "string"],
      interpolationRule,
      [/"/, "string.escape", "@popall"],
    ] as Array<Rule>,
    fStringBody: [
      [/[^']+/, "string"],
      interpolationRule,
      [/'/, "string.escape", "@popall"],
    ] as Array<Rule>,
  },
});

describe("patchPythonFStringEscapedBraces", () => {
  it("inserts escaped brace rules before interpolation in both f-string states", () => {
    const patched = patchPythonFStringEscapedBraces(buildLanguage());

    for (const stateName of ["fStringBody", "fDblStringBody"]) {
      const rules = patched.tokenizer[stateName] ?? [];
      const sources = rules.map((rule) => (rule[0] instanceof RegExp ? rule[0].source : String(rule[0])));
      const escapedOpenIndex = sources.indexOf("\\{\\{");
      const escapedCloseIndex = sources.indexOf("\\}\\}");
      const interpolationIndex = sources.indexOf("\\{[^\\}':!=]+");

      expect(escapedOpenIndex).toBeGreaterThanOrEqual(0);
      expect(escapedCloseIndex).toBeGreaterThan(escapedOpenIndex);
      expect(interpolationIndex).toBeGreaterThan(escapedCloseIndex);
    }
  });

  it("is idempotent when called multiple times", () => {
    const once = patchPythonFStringEscapedBraces(buildLanguage());
    const twice = patchPythonFStringEscapedBraces(once);

    for (const stateName of ["fStringBody", "fDblStringBody"]) {
      const sources = (twice.tokenizer[stateName] ?? []).map((rule) =>
        rule[0] instanceof RegExp ? rule[0].source : String(rule[0]),
      );

      expect(sources.filter((source) => source === "\\{\\{")).toHaveLength(1);
      expect(sources.filter((source) => source === "\\}\\}")).toHaveLength(1);
    }
  });
});
