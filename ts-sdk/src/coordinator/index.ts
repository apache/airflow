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

// Coordinator-mode public API. Re-exported through the package root.
//
// TaskClient and related types are exported from the package root. This
// barrel only exports coordinator-specific entry points.

export { startCoordinator, type StartCoordinatorOptions } from "./runtime.js";
/** Cadwyn schema version this SDK was generated against. Not sent on
 *  the wire — exposed so callers can read it for bundle metadata,
 *  health checks, or to confirm which schema their build is pinned to. */
export { SUPERVISOR_API_VERSION } from "./protocol.js";
