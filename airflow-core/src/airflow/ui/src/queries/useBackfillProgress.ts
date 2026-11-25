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
import { useBackfillServiceListBackfillDagRuns } from "openapi/queries";
import type { DagRunState } from "openapi/requests/types.gen";

import { useAutoRefresh } from "src/utils";

// Slots the backfill skipped have no Dag run, so they are left out of the total.
const CREATED_RUN_STATES: Array<DagRunState> = ["queued", "running", "success", "failed"];

export type BackfillProgress = {
  readonly failed: number;
  readonly success: number;
  readonly total: number;
};

// `limit: 0` returns only `total_entries`, so each count costs a single COUNT query
// however many runs the backfill has.
export const useBackfillProgress = (backfillId: number, isRunning: boolean): BackfillProgress | undefined => {
  const refetchInterval = useAutoRefresh({});
  // A completed backfill's counts are final, so it is fetched once rather than polled.
  const options = { refetchInterval: isRunning ? refetchInterval : false };

  const { data: total } = useBackfillServiceListBackfillDagRuns(
    { backfillId, limit: 0, state: CREATED_RUN_STATES },
    undefined,
    options,
  );
  const { data: success } = useBackfillServiceListBackfillDagRuns(
    { backfillId, limit: 0, state: ["success"] },
    undefined,
    options,
  );
  const { data: failed } = useBackfillServiceListBackfillDagRuns(
    { backfillId, limit: 0, state: ["failed"] },
    undefined,
    options,
  );

  if (total === undefined || success === undefined || failed === undefined) {
    return undefined;
  }

  return {
    failed: failed.total_entries,
    success: success.total_entries,
    total: total.total_entries,
  };
};
