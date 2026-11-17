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
/* eslint-disable @typescript-eslint/no-unsafe-enum-comparison -- AST node types are compared as strings so the plugin does not need `@typescript-eslint/utils` at runtime, which oxlint JS plugins cannot rely on. */

/**
 * @import { TSESLint, TSESTree } from "@typescript-eslint/utils";
 */

/** Reported by `@typescript-eslint/no-restricted-types` with a component-specific message instead. */
const componentTypes = new Set(["FC", "FunctionComponent", "VFC"]);

/**
 * Disallow the `React` type namespace. `@types/react` declares `export as namespace React`, so
 * `React.ReactNode` type-checks even in files that never import React.
 *
 * @type {TSESLint.RuleModule<"noReactNamespaceTypes">}
 */
export const noReactNamespaceTypes = {
  create: (context) => ({
    /** @param {TSESTree.TSQualifiedName} node */
    TSQualifiedName(node) {
      if (
        node.left.type === "Identifier" &&
        node.left.name === "React" &&
        !componentTypes.has(node.right.name)
      ) {
        context.report({ messageId: "noReactNamespaceTypes", node });
      }
    },
  }),
  defaultOptions: [],
  meta: {
    docs: {
      description: "Disallow qualifying React types with the `React` namespace",
    },
    messages: {
      noReactNamespaceTypes:
        'Import React types by name instead of qualifying them with the `React` namespace: `import { type ReactElement } from "react"`, then use `ReactElement`.',
    },
    schema: [],
    type: "problem",
  },
};
