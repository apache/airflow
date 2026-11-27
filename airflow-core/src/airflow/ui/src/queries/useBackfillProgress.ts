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
import { useQuery } from "@tanstack/react-query";

import { BackfillService } from "openapi/requests/services.gen";
import type { DagRunState } from "openapi/requests/types.gen";

import { useAutoRefresh } from "src/utils";

export const useBackfillProgressKey = "backfillProgress";

export type BackfillProgress = {
  readonly failed: number;
  readonly success: number;
  readonly total: number;
};

// `limit: 0` returns only `total_entries`, so each count costs a single COUNT query
// however many runs the backfill has.
const countDagRuns = async (backfillId: number, state: Array<DagRunState>) =>
  (await BackfillService.listBackfillDagRuns({ backfillId, limit: 0, state })).total_entries;

// Each count is its own request with its own snapshot, so the total is counted directly rather
// than summed from the per-state counts, which would drop a run that changes state in between.
const fetchBackfillProgress = async (backfillId: number): Promise<BackfillProgress> => {
  const [success, failed, total] = await Promise.all([
    countDagRuns(backfillId, ["success"]),
    countDagRuns(backfillId, ["failed"]),
    // Slots the backfill skipped have no Dag run, so they are left out of the total.
    countDagRuns(backfillId, ["queued", "running", "success", "failed"]),
  ]);

  return { failed, success, total };
};

export const useBackfillProgress = (backfillId: number) => {
  const refetchInterval = useAutoRefresh({});

  const { data } = useQuery({
    queryFn: () => fetchBackfillProgress(backfillId),
    queryKey: [useBackfillProgressKey, backfillId],
    refetchInterval,
  });

  return data;
};
