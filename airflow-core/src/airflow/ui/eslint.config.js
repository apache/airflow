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
import i18next from "eslint-plugin-i18next";
import oxlint from "eslint-plugin-oxlint";
import perfectionist from "eslint-plugin-perfectionist";

import { coreRules } from "./rules/core.js";
import { reactRules } from "./rules/react.js";
import { typescriptRules } from "./rules/typescript.js";
import { unicornRules } from "./rules/unicorn.js";

/**
 * ESLint configuration.
 * @see [ESLint configuration](https://eslint.org/docs/latest/use/configure/)
 */
export default /** @type {const} @satisfies {ReadonlyArray<FlatConfig.Config>} */ ([
  // Global ignore of dist directory
  { ignores: ["**/dist/", "**coverage/", "**/openapi-gen/", "**/.vite/**"] },
  // Most rules run in oxlint now, so ESLint would call their disable comments unused and `--fix` would
  // delete them.
  { linterOptions: { reportUnusedDisableDirectives: "off" } },
  // Base rules
  coreRules,
  typescriptRules,
  // Da rest
  reactRules,
  unicornRules,
  // Registered without rules so `eslint-disable` comments naming them still resolve; oxlint runs them.
  { plugins: { i18next, perfectionist } },
  // oxlint runs every rule it supports (see `.oxlintrc.json`, including the perfectionist, stylistic,
  // i18next and local `airflow` plugins); this turns those off here so ESLint only runs the rest.
  ...(await oxlint.buildFromConfigFile("./.oxlintrc.json")),
]);
