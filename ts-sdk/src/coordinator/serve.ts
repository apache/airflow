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

// What `bundle.serve()` does: the one-shot latch, and the call into the
// coordinator. Kept out of `sdk/bundle.ts` so the authoring surface never
// imports the coordinator, and out of `runtime.ts` so importing it back from
// the SDK side cannot close a cycle.

import { startCoordinator } from "./runtime.js";
import type { Bundle } from "../sdk/bundle.js";

// What must not happen twice is one process connecting two pairs of sockets, so
// this is keyed globally rather than held in a module variable: two resolved
// copies of the package would each get their own, and both would be first.
const SERVED = Symbol.for("airflow.ts-sdk.served");

function serveLatch(): Record<symbol, boolean | undefined> {
  return globalThis as unknown as Record<symbol, boolean | undefined>;
}

/**
 * Internal: serve `bundle` to Airflow. The latch lives here, with the sockets
 * it protects, rather than on the bundle that holds none.
 *
 * Not exported from the package root: a Dag author reaches the runtime through
 * `bundle.serve()`.
 */
export async function serveBundle(bundle: Bundle): Promise<void> {
  const latch = serveLatch();
  if (latch[SERVED]) {
    throw new Error(
      "bundle.serve() was already called; serve everything a bundle provides from a single bundle",
    );
  }
  // Set before the first await, so two concurrent calls cannot both pass.
  latch[SERVED] = true;
  try {
    await startCoordinator(bundle);
  } catch (err) {
    // startCoordinator closes both sockets on its way out, so a failed serve
    // holds nothing open and may be retried.
    latch[SERVED] = undefined;
    throw err;
  }
}
