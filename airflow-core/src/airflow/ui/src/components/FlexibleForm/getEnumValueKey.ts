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
import type { ParamValue } from "src/queries/useDagParams";

const sortKeys = (value: ParamValue): ParamValue => {
  if (Array.isArray(value)) {
    return value.map(sortKeys);
  }

  return typeof value === "object" && value !== null
    ? Object.fromEntries(
        Object.keys(value)
          .sort()
          .map((key) => [key, sortKeys(value[key] as ParamValue)]),
      )
    : value;
};

/**
 * String identity of an enum entry, also used as the `values_display` lookup key.
 * Objects and arrays are keyed by their JSON form with sorted keys and no whitespace, so structurally
 * equal values share one key regardless of how they were written; entries that differ only in key order
 * collapse into a single option.
 */
export const getEnumValueKey = (value: ParamValue): string =>
  typeof value === "object" && value !== null ? JSON.stringify(sortKeys(value)) : String(value);

const toCanonicalKey = (key: string): string => {
  try {
    return getEnumValueKey(JSON.parse(key) as ParamValue);
  } catch {
    return key;
  }
};

/**
 * Label for an enum key. Dag authors write object keys as JSON text (`json.dumps(...)`), so the lookup
 * accepts any spacing and key order rather than requiring the exact canonical form.
 */
export const lookupValuesDisplay = (
  key: string,
  valuesDisplay: Record<string, string> | undefined,
): string | undefined => {
  if (!valuesDisplay || typeof valuesDisplay !== "object") {
    return undefined;
  }
  if (key in valuesDisplay) {
    return valuesDisplay[key];
  }
  if (!key.startsWith("{") && !key.startsWith("[")) {
    return undefined;
  }
  const match = Object.keys(valuesDisplay).find((displayKey) => toCanonicalKey(displayKey) === key);

  return match === undefined ? undefined : valuesDisplay[match];
};
