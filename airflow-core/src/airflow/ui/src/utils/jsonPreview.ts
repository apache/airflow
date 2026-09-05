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

export type JsonPreviewEntry = {
  /** Stable React key: the object key, or the index for array items. */
  readonly id: string;
  /** Nested objects and arrays are only hinted at — their contents need the full editor. */
  readonly isComplex: boolean;
  /** Set when the badge stands in for a whole array; the caller renders a translated count. */
  readonly itemCount?: number;
  /** Absent for array items, which have nothing meaningful to label them with. */
  readonly label?: string;
  readonly value: string;
};

const isPrimitive = (value: unknown) =>
  value === null || ["boolean", "number", "string", "undefined"].includes(typeof value);

const formatValue = (value: unknown): Pick<JsonPreviewEntry, "isComplex" | "value"> => {
  if (typeof value === "string") {
    return { isComplex: false, value: value === "" ? '""' : value };
  }
  if (isPrimitive(value)) {
    return { isComplex: false, value: String(value) };
  }
  if (Array.isArray(value)) {
    return { isComplex: true, value: value.length === 0 ? "[]" : "[…]" };
  }

  return { isComplex: true, value: Object.keys(value as object).length === 0 ? "{}" : "{…}" };
};

/**
 * Flatten a JSON value into one badge per top-level entry, so a table cell can say
 * `score: 0.99` instead of an anonymous `{ ... }`.
 *
 * Returns `undefined` for empty content, which has nothing worth rendering at all.
 */
export const getJsonPreviewEntries = (content: object): Array<JsonPreviewEntry> | undefined => {
  if (Array.isArray(content)) {
    if (content.length === 0) {
      return undefined;
    }

    // Items nested in an array have no key to identify them, so one badge each would read
    // `{…} {…} {…}`. Summarise the array as a whole instead.
    if (!content.every((item: unknown) => isPrimitive(item))) {
      return [{ id: "items", isComplex: true, itemCount: content.length, value: "[…]" }];
    }

    return content.map((item: unknown, index) => ({ id: String(index), ...formatValue(item) }));
  }

  const entries = Object.entries(content);

  if (entries.length === 0) {
    return undefined;
  }

  return entries.map(([key, value]) => ({ id: key, label: key, ...formatValue(value) }));
};
