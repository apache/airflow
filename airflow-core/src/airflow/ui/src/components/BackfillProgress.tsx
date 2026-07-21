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
import { type InfiniteData, useInfiniteQuery } from "@tanstack/react-query";
import { useEffect } from "react";

import { UseBackfillServiceListBackfillDagRunsKeyFn } from "openapi/queries";
import { BackfillService } from "openapi/requests/services.gen";
import type { BackfillDagRunCollectionResponse } from "openapi/requests/types.gen";

import { BackfillProgressBar } from "./BackfillProgressBar";

type Props = {
  readonly backfillId: number;
  readonly isCompleted: boolean;
  readonly refetchInterval?: number | false;
  readonly trackColor?: string;
};

const PAGE_SIZE = 100;

export const BackfillProgress = ({ backfillId, isCompleted, refetchInterval, trackColor }: Props) => {
  const { data, fetchNextPage, hasNextPage, isFetchingNextPage } = useInfiniteQuery<
    BackfillDagRunCollectionResponse,
    unknown,
    InfiniteData<BackfillDagRunCollectionResponse>,
    Array<unknown>,
    number
  >({
    getNextPageParam: (lastPage, pages) => {
      const nextOffset = pages.reduce((count, page) => count + page.backfill_dag_runs.length, 0);

      return nextOffset < lastPage.total_entries ? nextOffset : undefined;
    },
    initialPageParam: 0,
    queryFn: ({ pageParam }) =>
      BackfillService.listBackfillDagRuns({ backfillId, limit: PAGE_SIZE, offset: pageParam }),
    queryKey: UseBackfillServiceListBackfillDagRunsKeyFn({ backfillId, limit: PAGE_SIZE }, [
      { backfillId, limit: PAGE_SIZE, pagination: "all" },
    ]),
    refetchInterval: isCompleted ? false : (refetchInterval ?? 5000),
  });

  useEffect(() => {
    if (hasNextPage && !isFetchingNextPage) {
      void fetchNextPage();
    }
  }, [fetchNextPage, hasNextPage, isFetchingNextPage]);

  const total = data?.pages.at(-1)?.total_entries ?? 0;

  if (!data || hasNextPage || isFetchingNextPage || total === 0) {
    return undefined;
  }

  return (
    <BackfillProgressBar
      dagRuns={data.pages.flatMap((page) => page.backfill_dag_runs)}
      total={total}
      trackColor={trackColor}
    />
  );
};
