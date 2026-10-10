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

import type { JsonValue } from "./client-types.js";
import { isPlainRecord } from "./dag.js";

/**
 * Copy a literal argument as plain JSON, without the `{__type, __var}` wrapper of Dag
 * serialization: Python writes a literal as it is, and the runtime hands it to the handler
 * undecoded.
 *
 * A value JSON cannot carry, such as a `Date` or a `Map`, is rejected rather
 * than reaching the handler as something else.
 */
export function toPlainJson(value: unknown, label: string): JsonValue {
  if (value === null || value === undefined) return null;
  if (typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (Array.isArray(value)) return value.map((item) => toPlainJson(item, label));
  if (isPlainRecord(value)) {
    const copy: Record<string, JsonValue> = {};
    for (const [key, item] of Object.entries(value)) copy[key] = toPlainJson(item, label);
    return copy;
  }
  throw new Error(
    `${label} holds ${describeType(value)}, which JSON cannot carry; pass a string, a finite ` +
      "number, a boolean, null, an array or a plain object",
  );
}

/** A value's type as a noun phrase for an error message, such as "a Date" or "an array". */
export function describeType(value: unknown): string {
  if (value === null) return "null";
  if (Array.isArray(value)) return "an array";
  if (typeof value === "number" && !Number.isFinite(value)) return String(value);
  const noun =
    typeof value === "object" && !isPlainRecord(value) ? getClassName(value) : typeof value;
  return `${/^[aeiou]/i.test(noun) ? "an" : "a"} ${noun}`;
}

function getClassName(value: object): string {
  const prototype = Object.getPrototypeOf(value) as { constructor?: { name?: string } } | null;
  return prototype?.constructor?.name || "object";
}
