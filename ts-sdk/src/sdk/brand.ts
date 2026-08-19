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

// Internal: tells an SDK object apart from a stray one, across copies.
//
// Brands live in the cross-realm symbol registry, which two resolved copies of
// this package share even though each gets its own class object. That does not
// make another copy's objects usable — `Dag` and `DagRegistry` read private
// state keyed to the class that declared it — so callers still guard with
// `instanceof` and use these only to tell the two failures apart.

const PREFIX = "airflow.ts-sdk.";

/** Mark `target` as built by this package. Not a declared field, so it stays
 *  out of the public type. */
export function brand(target: object, name: string): void {
  Object.defineProperty(target, Symbol.for(PREFIX + name), { value: true });
}

/** Whether `value` carries `name`'s brand, from any copy of this package. */
export function hasBrand(value: unknown, name: string): boolean {
  return typeof value === "object" && value !== null && Symbol.for(PREFIX + name) in value;
}

/** Tail shared by the errors reporting a second resolved copy. */
export const DUPLICATE_COPY_HINT =
  "comes from a different copy of @apache-airflow/ts-sdk; deduplicate the dependency so one copy is resolved";
