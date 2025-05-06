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

import type { TaskInstanceState } from "openapi/requests/types.gen";

export const taskInstanceStateOptions = createListCollection<{
  label: string;
  value: TaskInstanceState | "all" | "none";
}>({
  items: [
    { label: "All States", value: "all" },
    { label: "Scheduled", value: "scheduled" },
    { label: "Queued", value: "queued" },
    { label: "Running", value: "running" },
    { label: "Success", value: "success" },
    { label: "Restarting", value: "restarting" },
    { label: "Failed", value: "failed" },
    { label: "Up For Retry", value: "up_for_retry" },
    { label: "Up For Reschedule", value: "up_for_reschedule" },
    { label: "Upstream failed", value: "upstream_failed" },
    { label: "Skipped", value: "skipped" },
    { label: "Deferred", value: "deferred" },
    { label: "Removed", value: "removed" },
    { label: "No Status", value: "none" },
  ],
});

export const dagRunStateOptions = createListCollection({
  items: [
    { label: "All States", value: "all" },
    { label: "Queued", value: "queued" },
    { label: "Running", value: "running" },
    { label: "Failed", value: "failed" },
    { label: "Success", value: "success" },
  ],
});

export const dagRunTypeOptions = createListCollection({
  items: [
    { label: "All Run Types", value: "all" },
    { label: "Backfill", value: "backfill" },
    { label: "Manual", value: "manual" },
    { label: "Scheduled", value: "scheduled" },
    { label: "Asset Triggered", value: "asset_triggered" },
  ],
});
