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

// `configureMonaco` registers these grammars from Monaco's internal definition modules and
// guards their `{ conf, language }` export shape at runtime — but that guard only fires in the
// browser once the Code tab mounts. A Monaco upgrade that moved a module or dropped an export
// would otherwise slip through CI and only surface as silently-unhighlighted code in production.
// Importing the modules here fails the build loudly instead: a missing module breaks resolution,
// a dropped export fails the assertion. Python is covered separately by the tokenized suite in
// `pythonFStrings.test.ts`, which imports the same module and runs its grammar.
const grammars = [
  { id: "go", load: () => import("monaco-editor/languages/definitions/go/go") },
  { id: "java", load: () => import("monaco-editor/languages/definitions/java/java") },
  { id: "typescript", load: () => import("monaco-editor/languages/definitions/typescript/typescript") },
] as const;

describe("Monaco Dag-code grammars", () => {
  it.each(grammars)(
    "$id exposes the { conf, language } Monarch shape configureMonaco registers",
    async ({ load }) => {
      const { conf, language } = await load();

      expect(conf).toBeDefined();
      expect(language).toBeDefined();
      expect(language.tokenizer).toBeDefined();
    },
  );
});
