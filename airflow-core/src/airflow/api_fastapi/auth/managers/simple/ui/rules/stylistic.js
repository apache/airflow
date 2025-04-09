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

/**
 * @import { FlatConfig } from "@typescript-eslint/utils/ts-eslint";
 */
import stylistic from "@stylistic/eslint-plugin";

import { ERROR } from "./levels.js";

/**
 * ESLint `@stylistic/eslint-plugin` namespace.
 */
export const stylisticESLintNamespace = "@stylistic";

/**
 * ESLint `@stylistic/eslint-plugin` rules.
 * @see [@stylistic/eslint-plugin](https://eslint.style/packages/default#rules)
 */
export const stylisticRules = /** @type {const} @satisfies {FlatConfig.Config} */ ({
  plugins: { [stylisticESLintNamespace]: stylistic },
  rules: {
    /**
     * Require padding lines between statements:
     *
     * -   Always a new line before a `return` statement.
     * -   Always a new line after an `import`, `const`, `let` or `var` statement.
     * -   Indifferent if followed by same type (`const`, `let` or `var`).
     * -   Indifferent in `import` followed by other `import`.
     * -   Always a new line before an `export` statement.
     *
     * @see [padding-line-between-statements](https://eslint.style/rules/default/padding-line-between-statements)
     */
    [`${stylisticESLintNamespace}/padding-line-between-statements`]: [
      ERROR,
      {
        blankLine: "always",
        next: "return",
        prev: "*",
      },
      {
        blankLine: "always",
        next: "*",
        prev: ["import", "const", "let", "var"],
      },
      {
        blankLine: "any",
        next: ["const", "let", "var"],
        prev: ["const", "let", "var"],
      },
      {
        blankLine: "any",
        next: "import",
        prev: "import",
      },
      {
        blankLine: "always",
        next: "export",
        prev: "*",
      },
      {
        blankLine: "any",
        next: "export",
        prev: "export",
      },
    ],

    /**
     * Requires a whitespace (space or tab) beginning a comment.
     *
     * @see [spaced-comment](https://eslint.style/rules/default/spaced-comment)
     */
    [`${stylisticESLintNamespace}/spaced-comment`]: [ERROR, "always", { exceptions: ["!"] }],
  },
});
