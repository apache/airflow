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
import { InfiniteData, useInfiniteQuery, UseInfiniteQueryOptions } from "@tanstack/react-query";

import { DagService } from "openapi/requests/services.gen";
import { DAGTagCollectionResponse } from "openapi/requests/types.gen";

import * as Common from "./common";

export const useDagTagsInfinite = <TError = unknown, TQueryKey extends Array<unknown> = unknown[]>(
  {
    limit,
    orderBy,
    tagNamePattern,
  }: {
    limit?: number;
    orderBy?: string;
    tagNamePattern?: string;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<
    UseInfiniteQueryOptions<
      DAGTagCollectionResponse,
      TError,
      InfiniteData<DAGTagCollectionResponse>,
      DAGTagCollectionResponse,
      unknown[],
      number
    >,
    "queryKey" | "queryFn"
  >,
) =>
  useInfiniteQuery({
    queryKey: Common.UseDagServiceGetDagTagsKeyFn({ limit, orderBy, tagNamePattern }, queryKey),
    queryFn: ({ pageParam }) => DagService.getDagTags({ limit, offset: pageParam, orderBy, tagNamePattern }),
    initialPageParam: 0,
    getNextPageParam: (lastPage, _allPages, lastPageParam, _allPageParams) =>
      lastPageParam < lastPage.total_entries ? lastPage.tags.length + lastPageParam : undefined,
    getPreviousPageParam: (firstPage, _allPages, firstPageParam, _allPageParams) =>
      firstPageParam > 0 ? -firstPage.tags.length + firstPageParam : undefined,
    ...options,
  });
