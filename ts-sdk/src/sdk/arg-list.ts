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

// Giving a task's inputs in order instead of by name.

import { brand, hasBrand } from "./brand.js";

// Not a declared field, so the values stay out of the public type: an arg list
// is a carrier the Dag reads, never something an author takes apart. A global
// symbol, as the brands are, so two resolved copies agree on the key.
const VALUES = Symbol.for("airflow.ts-sdk.arg-list-values");

// Carries the value types without carrying a value, the way TaskRef carries
// its return type.
declare const LISTED_VALUES: unique symbol;

/**
 * A task's inputs in the order its handler destructures them, as
 * {@link withArgList} builds.
 */
export interface ArgList<TValues extends readonly unknown[] = readonly unknown[]> {
  /** @internal Never set; see {@link LISTED_VALUES}. */
  readonly [LISTED_VALUES]: TValues;
}

/**
 * Give a task's inputs in order instead of naming them.
 *
 * Naming the inputs is the usual way to call a task, and reads best past a
 * couple of arguments:
 *
 * ```ts
 * transform({ rows: extracted, region: "us" });
 * ```
 *
 * `withArgList` supplies the same inputs in order, for a call that reads
 * better that way. Each value binds to the argument in that position:
 *
 * ```ts
 * transform(withArgList(extracted, "us"));
 * ```
 *
 * The order is the one the handler destructures its argument in, so the
 * handler has to take a plain object pattern — `async ({ rows, region }) =>
 * ...`. A handler written any other way has no order to read, and its task is
 * called by name.
 */
export function withArgList<const TValues extends readonly unknown[]>(
  ...values: TValues
): ArgList<TValues> {
  const list = {};
  Object.defineProperty(list, VALUES, { value: Object.freeze([...values]) });
  brand(list, "ArgList");
  return Object.freeze(list) as ArgList<TValues>;
}

/** Internal: whether `value` is an arg list built by any copy of this package. */
export function isArgList(value: unknown): value is ArgList {
  return hasBrand(value, "ArgList");
}

/** Internal: the values an arg list carries, in the order they were given. */
export function argListValues(list: ArgList): readonly unknown[] {
  const values: unknown = (list as unknown as Record<symbol, unknown>)[VALUES];
  return Array.isArray(values) ? (values as readonly unknown[]) : [];
}
