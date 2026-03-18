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
import perfectionist from "eslint-plugin-perfectionist";

import { ERROR } from "./levels.js";
import { off } from "./off.js";

/**
 * ESLint `perfectionist` namespace.
 */
export const perfectionistNamespace = "perfectionist";

/**
 * ESLint `perfectionist` rules.
 * @see [perfectionist](https://perfectionist.dev/rules)
 */
export const perfectionistRules = /** @type {const} @satisfies {FlatConfig.Config} */ ({
  plugins: {
    [perfectionistNamespace]: perfectionist,
  },
  rules: {
    /**
     * Enforce sorted array values if the `includes` method is
     * immediately called after the array is created.
     *
     * @see [perfectionist/sort-array-includes](https://perfectionist.dev/rules/sort-array-includes)
     */
    [`${perfectionistNamespace}/sort-array-includes`]: ERROR,

    /**
     * Enforce sorted class members.
     *
     * @see [perfectionist/sort-classes](https://perfectionist.dev/rules/sort-classes)
     */
    [`${perfectionistNamespace}/sort-classes`]: ERROR,

    /**
     * Enforce sorted TypeScript enum members.
     *
     * @see [perfectionist/sort-enums](https://perfectionist.dev/rules/sort-enums)
     */
    [`${perfectionistNamespace}/sort-enums`]: ERROR,

    /**
     * Enforce sorted TypeScript interface properties.
     *
     * @see [perfectionist/sort-interfaces](https://perfectionist.dev/rules/sort-interfaces)
     */
    [`${perfectionistNamespace}/sort-interfaces`]: ERROR,

    /**
     * Enforce sorted intersection types in TypeScript.
     *
     * @see [perfectionist/sort-intersection-types](https://perfectionist.dev/rules/sort-intersection-types)
     */
    [`${perfectionistNamespace}/sort-intersection-types`]: ERROR,

    /**
     * Enforce sorted JSX props within JSX elements.
     *
     * @see [perfectionist/sort-jsx-props](https://perfectionist.dev/rules/sort-jsx-props)
     */
    [`${perfectionistNamespace}/sort-jsx-props`]: ERROR,

    /**
     * Enforce sorted elements within JavaScript Map object.
     *
     * @see [perfectionist/sort-maps](https://perfectionist.dev/rules/sort-maps)
     */
    [`${perfectionistNamespace}/sort-maps`]: ERROR,

    /**
     * Enforce sorted object types.
     *
     * @see [perfectionist/sort-object-types](https://perfectionist.dev/rules/sort-object-types)
     */
    [`${perfectionistNamespace}/sort-object-types`]: ERROR,

    /**
     * Enforce sorted object types.
     *
     * @see [perfectionist/sort-objects](https://perfectionist.dev/rules/sort-objects)
     */
    [`${perfectionistNamespace}/sort-objects`]: [
      ERROR,
      {
        type: "natural",
      },
    ],

    /**
     * Enforce sorted switch case statements.
     *
     * @see [perfectionist/sort-switch-case](https://perfectionist.dev/rules/sort-switch-case)
     */
    [`${perfectionistNamespace}/sort-switch-case`]: ERROR,

    /**
     * Enforce sorted TypeScript union types.
     *
     * nullish groups will leave null and undefined at the end
     *
     * @see [perfectionist/sort-union-types](https://perfectionist.dev/rules/sort-union-types)
     */
    [`${perfectionistNamespace}/sort-union-types`]: [
      ERROR,
      {
        groups: [
          ["conditional", "function", "import"],
          ["intersection", "keyword", "named"],
          ["literal", "object", "operator", "tuple", "union"],
          "nullish",
        ],
      },
    ],

    ...off("sort-keys"),
  },
});
