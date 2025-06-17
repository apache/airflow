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
