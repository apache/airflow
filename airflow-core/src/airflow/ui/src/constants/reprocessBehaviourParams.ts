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
import type { ReprocessBehavior } from "openapi/requests/types.gen";

import { isOneOf } from "src/utils";

export const reprocessBehaviors = [
  { label: "components:backfill.missingRuns", taskLevel: false, value: "none" },
  { label: "components:backfill.missingAndErroredRuns", taskLevel: false, value: "failed" },
  { label: "components:backfill.allRuns", taskLevel: false, value: "completed" },
  { label: "components:backfill.failedTasksOnly", taskLevel: true, value: "failed_tasks" },
  { label: "components:backfill.newTasksOnly", taskLevel: true, value: "new_tasks" },
] as const satisfies ReadonlyArray<{ label: string; taskLevel: boolean; value: ReprocessBehavior }>;

export const isReprocessBehavior = isOneOf(reprocessBehaviors.map((behavior) => behavior.value));
