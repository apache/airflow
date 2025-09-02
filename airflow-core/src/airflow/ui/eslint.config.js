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
import { coreRules } from "./rules/core.js";
import { i18nRules } from "./rules/i18n.js";
import { i18nextRules } from "./rules/i18next.js";
import { jsoncRules } from "./rules/jsonc.js";
import { perfectionistRules } from "./rules/perfectionist.js";
import { prettierRules } from "./rules/prettier.js";
import { reactRules } from "./rules/react.js";
import { stylisticRules } from "./rules/stylistic.js";
import { typescriptRules } from "./rules/typescript.js";
import { unicornRules } from "./rules/unicorn.js";

/**
 * ESLint configuration.
 * @see [ESLint configuration](https://eslint.org/docs/latest/use/configure/)
 */
export default /** @type {const} @satisfies {ReadonlyArray<FlatConfig.Config>} */ ([
  // Global ignore of dist directory
  { ignores: ["**/dist/", "**coverage/", "**/openapi-gen/", "**/.vite/**"] },
  // Base rules
  coreRules,
  typescriptRules,

  // Custom UI rules
  {
    files: ["src/**/*.{ts,tsx}"],
    rules: {
      // Nudge devs away from Chakra internals; use our app theme
      "no-restricted-imports": [
        "warn",
        {
          paths: [
            {
              message: "Import semantic tokens from '@/theme', not Chakra internals.",
              name: "@chakra-ui/theme",
            },
          ],
        },
      ],
      // Warn on Chakra palette (gray.500 etc.) or hex colors in color-like props.
      // NOTE: This intentionally allows semantic tokens like "border.emphasized", "accent.default", etc.
      "no-restricted-syntax": [
        "warn",
        // <Box color="gray.500"> (string literal)
        {
          message:
            "Use semantic tokens (e.g., 'surface', 'border', 'text.muted', 'accent.default', 'status.success') instead of raw hex or palette values.",
          selector:
            "JSXAttribute[name.name=/^(bg|background|backgroundColor|color|borderColor|fill|stroke)$/] > Literal[value=/^#|\\b(?:gray|blue|red|green|orange|yellow|purple|pink|teal|cyan|blackAlpha|whiteAlpha)\\.(?:\\d{2,3}|[a-z]+)\\b/]",
        },
        // <Box color={"gray.500"}> (wrapped literal)
        {
          message:
            "Use semantic tokens (e.g., 'surface', 'border', 'text.muted', 'accent.default', 'status.success') instead of raw hex or palette values.",
          selector:
            "JSXAttribute[name.name=/^(bg|background|backgroundColor|color|borderColor|fill|stroke)$/] > JSXExpressionContainer > Literal[value=/^#|\\b(?:gray|blue|red|green|orange|yellow|purple|pink|teal|cyan|blackAlpha|whiteAlpha)\\.(?:\\d{2,3}|[a-z]+)\\b/]",
        },
        // <Box sx={{ color: "gray.500" }}>
        {
          message:
            "Use semantic tokens (e.g., 'surface', 'border', 'text.muted', 'accent.default', 'status.success') instead of raw hex or palette values.",
          selector:
            "JSXAttribute[name.name='sx'] ObjectExpression > Property[key.name=/^(bg|background|backgroundColor|color|borderColor|fill|stroke)$/] > Literal[value=/^#|\\b(?:gray|blue|red|green|orange|yellow|purple|pink|teal|cyan|blackAlpha|whiteAlpha)\\.(?:\\d{2,3}|[a-z]+)\\b/]",
        },
        // <Box _hover={{ bg: "blue.500" }}>
        {
          message:
            "Use semantic tokens (e.g., 'surface', 'border', 'text.muted', 'accent.default', 'status.success') instead of raw hex or palette values.",
          selector:
            "JSXAttribute[name.name=/^_(hover|active|focus|selected|disabled)$/] ObjectExpression > Property[key.name=/^(bg|background|backgroundColor|color|borderColor|fill|stroke)$/] > Literal[value=/^#|\\b(?:gray|blue|red|green|orange|yellow|purple|pink|teal|cyan|blackAlpha|whiteAlpha)\\.(?:\\d{2,3}|[a-z]+)\\b/]",
        },
      ],
    },
  },

  // Allow brand/inline SVG assets to keep hex/palette if needed
  {
    files: ["src/assets/**/*.{ts,tsx}"],
    rules: {
      "no-restricted-syntax": "off",
    },
  },

  // Da rest
  perfectionistRules,
  prettierRules,
  reactRules,
  stylisticRules,
  unicornRules,
  i18nextRules,
  i18nRules,
  jsoncRules,
]);
