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

import { configureMonaco } from "./configureMonaco";

// A Monaco upgrade that moved a module or dropped an export
// would slip through CI and only surface as silently-unhighlighted code in production.
// Importing the modules here fails the build loudly instead: a missing module breaks resolution,
// a dropped export fails the assertion.
describe("Monaco Dag-code grammars", () => {
  // Any language id an importer stores must be highlightable, not only the ones core ships.
  it.each(["go", "java", "python", "rust", "typescript", "yaml"])(
    "registers %s for lazy loading",
    async (languageId) => {
      await configureMonaco();
      const { languages } = await import("monaco-editor/editor");

      expect(languages.getLanguages().map(({ id }) => id)).toContain(languageId);
    },
    30_000,
  );

  it("tokenizes Python with the patched f-string grammar", async () => {
    await configureMonaco();
    const { editor } = await import("monaco-editor/editor");

    // Resolves the lazy tokenizer, which `tokenize` alone does not trigger.
    await editor.colorize("", "python", {});
    const [, escapedBracesLine] = editor.tokenize('x = f"""\n{{a}}\n"""', "python");

    expect(escapedBracesLine).toEqual([{ language: "python", offset: 0, type: "string.python" }]);
  }, 30_000);
});
