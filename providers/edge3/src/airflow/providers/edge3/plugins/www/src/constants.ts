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
import { $EdgeWorkerState } from "../openapi-gen/requests/schemas.gen";

// Helper function to convert snake_case or space-separated strings to Title Case
const toTitleCase = (str: string): string => {
  return str
    .split(/[\s_]+/)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join(" ");
};

export const workerStateOptions = createListCollection<{
  label: string;
  value: EdgeWorkerState | "all";
}>({
  items: [
    { label: "All States", value: "all" },
    ...$EdgeWorkerState.enum.map((state) => ({
      label: toTitleCase(state),
      value: state as EdgeWorkerState,
    })),
  ],
});
