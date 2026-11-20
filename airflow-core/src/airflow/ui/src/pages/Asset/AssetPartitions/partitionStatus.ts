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
import type { AssetPartitionResponse, TaskInstanceState } from "openapi/requests/types.gen";

export type PartitionStatus = AssetPartitionResponse["status"];

export const PARTITION_STATUSES: Array<PartitionStatus> = ["materialized", "in_progress", "failed"];

// A partition has no state of its own: it borrows the colour and icon of the run state it stands for.
export const RUN_STATE_BY_STATUS: Record<PartitionStatus, TaskInstanceState> = {
  failed: "failed",
  in_progress: "running",
  materialized: "success",
};

export const STATUS_LABEL_KEY: Record<PartitionStatus, string> = {
  failed: "common:states.failed",
  in_progress: "assets:partitions.status.in_progress",
  materialized: "assets:partitions.status.materialized",
};
