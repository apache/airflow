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

export { registerTask, listRegisteredTasks } from "./sdk/registry.js";
export { VariableNotFoundError } from "./sdk/client.js";
export { startCoordinator, SUPERVISOR_API_VERSION } from "./coordinator/index.js";
export type { TaskClient } from "./sdk/client.js";
export type { ConnectionResult, GetXComOpts, JsonValue, SetXComOpts } from "./sdk/client-types.js";
export type { StartCoordinatorOptions } from "./coordinator/index.js";
export type { TaskRegistration } from "./sdk/registry.js";
export type { TaskContext, TaskHandler, TaskHandlerArgs } from "./sdk/task.js";
