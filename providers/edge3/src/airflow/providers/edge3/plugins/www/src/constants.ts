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

import { createListCollection } from "@chakra-ui/react";
import type { EdgeWorkerState } from "../openapi-gen/requests/types.gen";

export const workerStateOptions = createListCollection<{
  label: string;
  value: EdgeWorkerState | "all";
}>({
  items: [
    { label: "All States", value: "all" },
    { label: "Starting", value: "starting" },
    { label: "Running", value: "running" },
    { label: "Idle", value: "idle" },
    { label: "Shutdown Request", value: "shutdown request" },
    { label: "Terminating", value: "terminating" },
    { label: "Offline", value: "offline" },
    { label: "Unknown", value: "unknown" },
    { label: "Maintenance Request", value: "maintenance request" },
    { label: "Maintenance Pending", value: "maintenance pending" },
    { label: "Maintenance Mode", value: "maintenance mode" },
    { label: "Maintenance Exit", value: "maintenance exit" },
    { label: "Offline Maintenance", value: "offline maintenance" },
  ],
});
