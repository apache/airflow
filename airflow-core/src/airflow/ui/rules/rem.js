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
import { AST_NODE_TYPES } from "@typescript-eslint/utils";

export const remNamespace = "rem";

/**
 * Check if a value contains rem units
 * @param {string} value
 * @returns {boolean}
 */
const containsRem = (value) => /\d+\.?\d*rem/u.test(value);

/**
 * Convert rem value to pixels (1rem = 16px)
 * @param {string} value
 * @returns {string}
 */
const convertRemToPixels = (value) =>
  value.replaceAll(/(?<temp1>\d+\.?\d*)rem/gu, (_, number) => {
    if (typeof number === "string") {
      const pixels = parseFloat(number) * 16;

      return `${pixels}px`;
    }

    return value;
  });

export const remPlugin = {
  rules: {
    "no-rem-in-props": {
      /** @param {import('@typescript-eslint/utils').TSESLint.RuleContext<'noRemInProps', []>} context */
      create(context) {
        /** @param {import('@typescript-eslint/utils').TSESTree.JSXOpeningElement} node */
        const checkAttributes = (node) => {
          // Check all attributes for rem values in size, width, height props (but not style)
          node.attributes.forEach((attr) => {
            if (attr.type !== AST_NODE_TYPES.JSXAttribute || !attr.value) {
              return;
            }

            const attrName = attr.name.name;

            // Skip style attributes - rem is allowed there
            if (attrName === "style") {
              return;
            }

            // Only check size, width, height attributes
            if (attrName !== "height" && attrName !== "size" && attrName !== "width") {
              return;
            }

            let attrValue = undefined;

            // Handle different attribute value types
            if (attr.value.type === AST_NODE_TYPES.Literal) {
              attrValue = attr.value.value;
            } else if (
              attr.value.type === AST_NODE_TYPES.JSXExpressionContainer &&
              attr.value.expression.type === AST_NODE_TYPES.Literal
            ) {
              attrValue = attr.value.expression.value;
            }

            // Check for rem values
            if (typeof attrValue === "string" && containsRem(attrValue)) {
              const fixedValue = convertRemToPixels(attrValue);

              context.report({
                data: {
                  attribute: attrName,
                  fixedValue,
                  value: attrValue,
                },
                fix(fixer) {
                  // For string literals, replace the entire value
                  if (attr.value !== null && attr.value.type === AST_NODE_TYPES.Literal) {
                    return fixer.replaceText(attr.value, `{${fixedValue}}`);
                  }
                  // For JSX expressions with literal values, replace just the literal
                  if (
                    attr.value !== null &&
                    attr.value.type === AST_NODE_TYPES.JSXExpressionContainer &&
                    attr.value.expression.type === AST_NODE_TYPES.Literal
                  ) {
                    return fixer.replaceText(attr.value.expression, fixedValue);
                  }

                  // eslint-disable-next-line unicorn/no-null
                  return null;
                },
                messageId: "noRemInProps",
                node: attr,
              });
            }
          });
        };

        return {
          /** @param {import('@typescript-eslint/utils').TSESTree.JSXOpeningElement} node */
          JSXOpeningElement(node) {
            checkAttributes(node);
          },
        };
      },
      meta: {
        docs: {
          category: "Best Practices",
          description: "Disallow rem units in size, width, and height attributes (but allow in style)",
          recommended: "error",
        },
        fixable: "code",
        messages: {
          noRemInProps:
            "Avoid using rem units in {{attribute}} attribute. Use numeric pixel values instead of '{{value}}'. Auto-fix available: {{fixedValue}}",
        },
        type: "problem",
      },
    },
  },
};

/** @type {import("@typescript-eslint/utils/ts-eslint").FlatConfig.Config} */
export const remRules = {
  files: ["**/*.tsx"],
  plugins: {
    [remNamespace]: remPlugin,
  },
  rules: {
    [`${remNamespace}/no-rem-in-props`]: "error",
  },
};
