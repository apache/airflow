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
  { ignores: ["**/dist/", "**coverage/"] },
  // Base rules
  coreRules,
  typescriptRules,
  // Da rest
  perfectionistRules,
  prettierRules,
  reactRules,
  stylisticRules,
  unicornRules,
]);
