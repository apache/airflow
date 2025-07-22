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
    { label: "dags:filters.allStates", value: "all" },
    { label: "common:states.scheduled", value: "scheduled" },
    { label: "common:states.queued", value: "queued" },
    { label: "common:states.running", value: "running" },
    { label: "common:states.success", value: "success" },
    { label: "common:states.restarting", value: "restarting" },
    { label: "common:states.failed", value: "failed" },
    { label: "common:states.up_for_retry", value: "up_for_retry" },
    { label: "common:states.up_for_reschedule", value: "up_for_reschedule" },
    { label: "common:states.upstream_failed", value: "upstream_failed" },
    { label: "common:states.skipped", value: "skipped" },
    { label: "common:states.deferred", value: "deferred" },
    { label: "common:states.removed", value: "removed" },
    { label: "common:states.none", value: "none" },
  ],
});

export const dagRunStateOptions = createListCollection({
  items: [
    { label: "dags:filters.allStates", value: "all" },
    { label: "common:states.queued", value: "queued" },
    { label: "common:states.running", value: "running" },
    { label: "common:states.failed", value: "failed" },
    { label: "common:states.success", value: "success" },
  ],
});

export const dagRunTypeOptions = createListCollection({
  items: [
    { label: "dags:filters.allRunTypes", value: "all" },
    { label: "common:runTypes.backfill", value: "backfill" },
    { label: "common:runTypes.manual", value: "manual" },
    { label: "common:runTypes.scheduled", value: "scheduled" },
    { label: "common:runTypes.asset_triggered", value: "asset_triggered" },
  ],
});
