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
import i18nextPlugin from "eslint-plugin-i18next";

import { ERROR } from "./levels.js";

const allExtensions = "*.{j,t}s{x,}";

/**
 * ESLint rules for i18next to enforce internationalization best practices.
 * This is a customized configuration.
 *
 * @see [eslint-plugin-i18next](https://github.com/edvardchen/eslint-plugin-i18next)
 */
export const i18nextRules = /** @type {const} @satisfies {FlatConfig.Config} */ ({
  files: [
    // Check files in the ui/src directory
    `src/**/${allExtensions}`,
  ],
  ignores: [
    // Ignore test files
    "src/**/*.test.tsx",
  ],
  plugins: {
    i18next: i18nextPlugin,
  },
  rules: {
    /**
     * Enforce no literal strings in JSX/TSX markup.
     * This rule helps ensure all user-facing strings are properly internationalized.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * <div>Hello, world!</div>
     *
     * // ✅ Correct
     * <div>{translate('greeting')}</div>
     * ```
     * @see [i18next/no-literal-string](https://github.com/edvardchen/eslint-plugin-i18next#no-literal-string)
     */
    "i18next/no-literal-string": [
      ERROR,
      {
        markupOnly: true,
      },
    ],
  },
});
