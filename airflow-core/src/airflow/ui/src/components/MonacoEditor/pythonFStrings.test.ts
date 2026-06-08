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
import { beforeAll, describe, expect, it } from "vitest";

import { patchPythonFStrings } from "./pythonFStrings";

// A minimal stand-in for Monaco's bundled Python grammar: enough states for the
// patch's references to resolve and for the immutability check below. It is not a
// faithful copy of the real `strings` state (which the patch replaces wholesale).
const buildBaseLanguage = (): languages.IMonarchLanguage => ({
  defaultToken: "",
  tokenizer: {
    fDblStringBody: [[/"/u, "string.escape", "@popall"]],
    fStringBody: [[/'/u, "string.escape", "@popall"]],
    fStringDetail: [[/x/u, "identifier", "@pop"]],
    root: [{ include: "@strings" }],
    strings: [
      [/f"{1,3}/u, "string.escape", "@fDblStringBody"],
      [/f'{1,3}/u, "string.escape", "@fStringBody"],
    ],
  },
});

const firstSources = (rules: Array<languages.IMonarchLanguageRule>): Array<string> =>
  rules.map((rule) => (Array.isArray(rule) && rule[0] instanceof RegExp ? rule[0].source : ""));

const typesAt = (lines: Array<Array<string>>, index: number): Array<string> => lines[index] ?? [];

const tokenTypes = (line: Array<{ type: string }>): Array<string> => line.map((token) => token.type);

describe("patchPythonFStrings", () => {
  it("does not mutate the input grammar", () => {
    const base = buildBaseLanguage();
    const snapshot = JSON.stringify(base, (_key, value: unknown) =>
      value instanceof RegExp ? value.source : value,
    );

    patchPythonFStrings(base);

    expect(
      JSON.stringify(base, (_key, value: unknown) => (value instanceof RegExp ? value.source : value)),
    ).toBe(snapshot);
  });

  it("adds dedicated triple-quoted f-string states for both quote styles", () => {
    const patched = patchPythonFStrings(buildBaseLanguage());

    expect(patched.tokenizer.fStringBodyTriple).toBeDefined();
    expect(patched.tokenizer.fDblStringBodyTriple).toBeDefined();
  });

  it("preserves the base single-line f-string states", () => {
    const patched = patchPythonFStrings(buildBaseLanguage());

    expect(patched.tokenizer.fStringBody).toBeDefined();
    expect(patched.tokenizer.fDblStringBody).toBeDefined();
    expect(patched.tokenizer.fStringDetail).toBeDefined();
  });

  it.each(["fStringBody", "fDblStringBody"])(
    "prepends escaped-brace rules to the single-line %s state",
    (state) => {
      const patched = patchPythonFStrings(buildBaseLanguage());
      const sources = firstSources(patched.tokenizer[state] as Array<languages.IMonarchLanguageRule>);

      expect(sources[0]).toBe("\\{\\{");
      expect(sources[1]).toBe("\\}\\}");
    },
  );

  it("routes triple quotes before single quotes in the strings state", () => {
    const patched = patchPythonFStrings(buildBaseLanguage());
    const sources = firstSources(patched.tokenizer.strings as Array<languages.IMonarchLanguageRule>);

    const tripleDbl = sources.findIndex((source) => source.includes('"""'));
    const singleDbl = sources.indexOf('[Ff]"');
    const tripleSingle = sources.findIndex((source) => source.includes("'''"));
    const singleSingle = sources.indexOf("[Ff]'");

    expect(tripleDbl).toBeGreaterThanOrEqual(0);
    expect(tripleSingle).toBeGreaterThanOrEqual(0);
    expect(tripleDbl).toBeLessThan(singleDbl);
    expect(tripleSingle).toBeLessThan(singleSingle);
  });

  it.each([
    ["fDblStringBodyTriple", '"""'],
    ["fStringBodyTriple", "'''"],
  ])("closes %s on its triple quote and matches escaped braces before interpolation", (state, quote) => {
    const patched = patchPythonFStrings(buildBaseLanguage());
    const sources = firstSources(patched.tokenizer[state] as Array<languages.IMonarchLanguageRule>);

    const close = sources.findIndex((source) => source.includes(quote));
    const escapedOpen = sources.findIndex((source) => source.includes("\\{\\{"));
    const interpolation = sources.findIndex((source) => source.includes("[^!':=}]"));
    const body = sources.findIndex((source) => source.startsWith("[^"));

    expect(close).toBe(0);
    expect(close).toBeLessThan(body);
    expect(escapedOpen).toBeGreaterThanOrEqual(0);
    expect(escapedOpen).toBeLessThan(interpolation);
  });
});

// Exercises the patched grammar through Monaco's real Monarch tokenizer to prove
// the fix end to end. Mirrors files/dags/fstring_repro.py from issue #67986.
// Loads full monaco and registers the language once for the whole suite.
describe("patchPythonFStrings (tokenized)", () => {
  let lines: Array<Array<string>> = [];
  let singleLineTokens: Array<{ offset: number; type: string }> = [];

  beforeAll(async () => {
    const monaco = await import("monaco-editor/esm/vs/editor/editor.api");
    const { conf, language } = await import("monaco-editor/esm/vs/basic-languages/python/python.js");

    monaco.languages.register({ id: "python" });
    monaco.languages.setLanguageConfiguration("python", conf);
    monaco.languages.setMonarchTokensProvider("python", patchPythonFStrings(language));

    const source = [
      '    sql = f"""',
      "    SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END",
      "    FROM {table_name}",
      "    WHERE ds = '{{{{ ds }}}}'",
      '    """',
      "    return sql",
    ].join("\n");

    lines = monaco.editor.tokenize(source, "python").map(tokenTypes);

    // Single-line f-string: `{{` at offset 2 (after `f"`) is a literal brace,
    // `{var}` is a real interpolation.
    singleLineTokens = monaco.editor.tokenize('f"{{lit}} {var}"', "python")[0] ?? [];
  }, 30_000);

  it("keeps the whole multi-line f-string body string-colored", () => {
    // A plain SQL line and the interpolation line both stay inside the string,
    // rather than being re-tokenized as Python code after the first line.
    expect(typesAt(lines, 1)).toEqual(["string.python"]);
    expect(typesAt(lines, 2)).toContain("string.python");
    // Escaped braces `{{{{ ds }}}}` are literal string content, not interpolation.
    expect(typesAt(lines, 3)).toEqual(["string.python"]);
  });

  it("colors interpolations inside the f-string as identifiers", () => {
    // `FROM ` is string, `{table_name}` is an interpolation.
    expect(typesAt(lines, 2)).toContain("identifier.python");
  });

  it("ends the string at the closing triple quote so following code is not string-colored", () => {
    // The closing `"""` terminates the string...
    expect(typesAt(lines, 4)).toContain("string.escape.python");
    // ...so `return sql` is real Python again, not part of the string.
    expect(typesAt(lines, 5)).toContain("keyword.python");
    expect(typesAt(lines, 5)).not.toContain("string.python");
  });

  it("treats single-line escaped braces as string and real interpolation as identifier", () => {
    // `{{` (offset 2) is a literal brace rendered as string, not interpolation.
    const braceToken = singleLineTokens.find((token) => token.offset === 2);

    expect(braceToken?.type).toBe("string.python");
    // `{var}` is still a real interpolation.
    expect(singleLineTokens.some((token) => token.type === "identifier.python")).toBe(true);
  });
});
