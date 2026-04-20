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
import type { GridRunsResponse } from "openapi/requests";
import { useGridRuns } from "src/queries/useGridRuns.ts";

type Params = {
  gridRuns: Array<GridRunsResponse> | undefined;
  limit: number;
  offset: number;
  setOffset: (value: number) => void;
};

export type PaginationState = {
  hasNewerRuns: boolean;
  hasOlderRuns: boolean;
  latestNotVisible: boolean;
};

type Result = {
  handleNewerRuns: () => void;
  handleOlderRuns: () => void;
} & PaginationState;

/**
 * Pure helper — derives pagination flags from already-fetched data.
 * Kept separate so it can be tested without React or network dependencies.
 */
export const computePaginationState = ({
  gridRuns,
  latestRunId,
  limit,
  offset,
}: {
  gridRuns: Array<GridRunsResponse> | undefined;
  latestRunId: string | undefined;
  limit: number;
  offset: number;
}): PaginationState => ({
  hasNewerRuns: offset > 0,
  hasOlderRuns: (gridRuns?.length ?? 0) > limit,
  latestNotVisible:
    latestRunId !== undefined && gridRuns !== undefined && !gridRuns.some((dr) => dr.run_id === latestRunId),
});

/**
 * Encapsulates the pagination state for the grid: whether there are older/newer
 * pages available, whether the single most-recent run is off-screen, and the
 * handlers that advance the offset in either direction.
 */
export const useGridPagination = ({ gridRuns, limit, offset, setOffset }: Params): Result => {
  // Fetch only the latest run (no filters) so we can tell whether it is
  // already visible in the current grid window.
  const { data: latestRuns } = useGridRuns({ limit: 1 });
  const latestRunId = latestRuns?.[0]?.run_id;

  return {
    ...computePaginationState({ gridRuns, latestRunId, limit, offset }),
    handleNewerRuns: () => setOffset(Math.max(0, offset - limit)),
    handleOlderRuns: () => setOffset(offset + limit),
  };
};
