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

// Cross-realm slot recording the source file of the currently-loading module.
//
// `airflow-ts-pack` prepends a single line to each author-owned source file
// that writes the file's path into this slot before the module's non-import
// statements run. `Dag`'s constructor reads it so a Dag declared in
// `src/dags/reports.ts` carries that path even though esbuild has inlined
// every source file into one bundle.

/**
 * Key `airflow-ts-pack` writes into `globalThis[Symbol.for(...)]` to tag the
 * currently-loading module with its source file path. Exported so the pack
 * step and this reader use one source of truth.
 *
 * Keyed on a global symbol, as the serve latch is: two resolved copies of the
 * package would otherwise write to and read from different slots, and a Dag
 * constructed through one copy would carry no source when read through the
 * other.
 */
export const MODULE_SOURCE_SLOT_KEY = "airflow.ts-sdk.current-module-source";

const SLOT = Symbol.for(MODULE_SOURCE_SLOT_KEY);

/**
 * The source file the currently-loading module was compiled from, or
 * `undefined` outside a packed module load.
 *
 * Written by `airflow-ts-pack`'s module-source tag, one per author-owned
 * source file, run before the module's non-import statements. Read here by
 * `Dag`'s constructor.
 */
export function getCurrentModuleSource(): string | undefined {
  return (globalThis as Record<symbol, string | undefined>)[SLOT];
}
