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
// `airflow-ts-pack` prepends a save-and-set to each author-owned source file
// and appends a restore, so the slot holds the currently-loading module's
// path throughout that module's top-level code — and returns to the previous
// path when the module completes. `Dag`'s constructor reads the slot so a
// Dag declared in `src/dags/reports.ts` carries that path even though
// esbuild has inlined every source file into one bundle.
//
// What the save/restore covers:
//   - Nested imports. When A imports B, B's tag pushes "B" and its epilog
//     pops back to "A"; A's `new Dag(...)` after the import still sees "A".
//   - Top-level `await` within a single module. The slot stays this module's
//     path across the suspension, so a Dag constructed after the await is
//     still attributed here.
//
// What it does NOT cover:
//   - A Dag constructed in a scheduled callback (`setTimeout`, `queueMicrotask`,
//     a `.then(...)`) that runs after the module's synchronous top-level has
//     returned. The epilog has already restored the previous slot value, so
//     the Dag is attributed to whichever module the slot points to when the
//     callback fires — likely the wrong one.
//   - Two modules genuinely evaluating concurrently, e.g. through a deliberate
//     `Promise.all([import("./a.js"), import("./b.js")])`. Their save/restore
//     stacks interleave and either can end up seeing the other's path.
//
// Declare Dags at module top level (with or without a preceding await) and
// the mapping is stable; any Dag construction from a later callback lands in
// `dag_source_paths` under whichever module ran most recently.

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
