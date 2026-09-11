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
import { type InfiniteData, useInfiniteQuery, type UseInfiniteQueryOptions } from "@tanstack/react-query";

import { UseDagServiceGetDagTimetableTypesUiKeyFn } from "openapi/queries";
import { DagService } from "openapi/requests/services.gen";
import type { DagTimetableTypeCollectionResponse } from "openapi/requests/types.gen";

export const useDagTimetableTypesInfinite = <TError = unknown>(
  {
    limit,
    timetableTypePrefixPattern,
  }: {
    limit?: number;
    timetableTypePrefixPattern?: string;
  } = {},
  queryKey?: Array<unknown>,
  options?: Omit<
    UseInfiniteQueryOptions<
      DagTimetableTypeCollectionResponse,
      TError,
      InfiniteData<DagTimetableTypeCollectionResponse>,
      Array<unknown>,
      number
    >,
    "queryFn" | "queryKey"
  >,
) =>
  useInfiniteQuery({
    getNextPageParam: (
      lastPage: DagTimetableTypeCollectionResponse,
      _allPages: Array<DagTimetableTypeCollectionResponse>,
      lastPageParam: number,
    ) =>
      lastPageParam + lastPage.timetable_types.length < lastPage.total_entries
        ? lastPageParam + lastPage.timetable_types.length
        : undefined,
    getPreviousPageParam: (
      firstPage: DagTimetableTypeCollectionResponse,
      _allPages: Array<DagTimetableTypeCollectionResponse>,
      firstPageParam: number,
    ) => (firstPageParam > 0 ? Math.max(0, firstPageParam - firstPage.timetable_types.length) : undefined),
    initialPageParam: 0,
    queryFn: ({ pageParam }: { pageParam: number }) =>
      DagService.getDagTimetableTypesUi({
        limit,
        offset: pageParam,
        timetableTypePrefixPattern,
      }),
    queryKey: UseDagServiceGetDagTimetableTypesUiKeyFn({ limit, timetableTypePrefixPattern }, queryKey),
    ...options,
  });
