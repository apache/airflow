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
import prettier from "eslint-plugin-prettier";

import { off } from "./off.js";

/**
 * ESLint TypeScript namespace.
 */
export const prettierNamespace = "prettier";

/**
 * ESLint Prettier rules.
 * @see [eslint-plugin-prettier](https://github.com/prettier/eslint-plugin-prettier)
 */
export const prettierRules = /** @type {const} @satisfies {FlatConfig.Config} */ ({
  plugins: { [prettierNamespace]: prettier },
  rules: off("no-irregular-whitespace", "no-unexpected-multiline"),
});
