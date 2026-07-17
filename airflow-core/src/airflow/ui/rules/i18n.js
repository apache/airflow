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

/* eslint-disable @typescript-eslint/no-unsafe-argument */

/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import jsoncParser from "jsonc-eslint-parser";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const i18nNamespace = "i18n";
/**
 * Extract all nested keys from translation object
 * @param {Record<string, any>} obj
 * @param {string} [prefix]
 * @returns {string[]}
 */
const getKeys = (obj, prefix = "") => {
  if (Array.isArray(obj)) {
    return [];
  }

  return Object.keys(obj).flatMap((key) => {
    const newPrefix = prefix ? `${prefix}.${key}` : key;
    const value = obj[key];

    if (typeof value === "object" && value !== null && !Array.isArray(value)) {
      return [newPrefix, ...getKeys(value, newPrefix)];
    }

    return [newPrefix];
  });
};

// Path to locales directory
const localesDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../public/i18n/locales");

// Default language (English) as reference
const defaultLanguage = "en";
const defaultLanguageKeys = /** @type {Record<string, string[]>} */ ({});
const defaultLanguageDir = path.join(localesDir, defaultLanguage);

// Load translation keys from default language files
fs.readdirSync(defaultLanguageDir)
  .filter((file) => file.endsWith(".json"))
  .forEach((jsonFile) => {
    const ns = path.basename(jsonFile, ".json");
    const filePath = path.join(defaultLanguageDir, jsonFile);
    const fileContent = fs.readFileSync(filePath, "utf8");
    const parsedJson = JSON.parse(fileContent);

    if (typeof parsedJson === "object" && parsedJson !== null && !Array.isArray(parsedJson)) {
      defaultLanguageKeys[ns] = getKeys(parsedJson);
    }
  });

export const i18nPlugin = {
  files: ["public/i18n/locales/**/*.json"],
  rules: {
    "check-translations-completeness": {
      /** @param {import('@typescript-eslint/utils').TSESLint.RuleContext<'missingKeys' | 'fileError', []>} context */
      create(context) {
        return {
          /** @param {import('@typescript-eslint/utils').TSESTree.Program} node */
          Program(node) {
            // Get language code and namespace from file path
            const currentFilePath = context.filename;
            const langCode = path.dirname(path.relative(localesDir, currentFilePath));
            const namespace = path.basename(currentFilePath, ".json");

            if (langCode === defaultLanguage) {
              return;
            }

            // Get keys from current file
            const referenceKeys = defaultLanguageKeys[namespace];
            let langKeys;

            try {
              const parsedLangJson = JSON.parse(context.sourceCode.text);

              if (
                typeof parsedLangJson === "object" &&
                parsedLangJson !== null &&
                !Array.isArray(parsedLangJson)
              ) {
                langKeys = getKeys(parsedLangJson);
              } else {
                context.report({
                  data: { error: "Invalid JSON object.", filePath: currentFilePath },
                  messageId: "fileError",
                  node,
                });

                return;
              }
            } catch (error) {
              const message = error instanceof Error ? error.message : String(error);

              context.report({
                data: { error: message, filePath: currentFilePath },
                messageId: "fileError",
                node,
              });

              return;
            }

            // Check for missing translations
            const langKeysSet = new Set(langKeys);
            const missingKeys = referenceKeys.filter((key) => !langKeysSet.has(key));

            if (missingKeys.length > 0) {
              context.report({
                data: { keys: missingKeys.join(", "), lang: langCode, namespace },
                messageId: "missingKeys",
                node,
              });
            }
          },
        };
      },
      meta: {
        docs: {
          category: "Best Practices",
          description: "Ensures non-default lang files have all keys from default.",
          recommended: "warn",
        },
        messages: {
          fileError: "Failed to read/parse {{filePath}}. Error: {{error}}",
          missingKeys: "Lang '{{lang}}' (namespace: {{namespace}}) missing keys: {{keys}}",
        },
        type: "problem",
      },
    },
  },
};

/** @type {import("@typescript-eslint/utils/ts-eslint").FlatConfig.Config} */
export const i18nRules = {
  files: ["public/i18n/locales/**/*.json"],
  languageOptions: {
    parser: jsoncParser,
    parserOptions: {
      extraFileExtensions: [".json"],
    },
  },
  plugins: {
    [i18nNamespace]: i18nPlugin,
  },
  rules: {
    [`${i18nNamespace}/check-translations-completeness`]: "warn",
  },
};
