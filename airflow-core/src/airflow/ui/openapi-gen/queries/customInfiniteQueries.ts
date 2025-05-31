import { InfiniteData, useInfiniteQuery, UseInfiniteQueryOptions } from "@tanstack/react-query";

import { DagService } from "openapi/requests/services.gen";

import * as Common from "./common";

export const useDagServiceGetDagTagsInfinite = <
  TData = InfiniteData<Common.DagServiceGetDagTagsDefaultResponse>,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
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
  options?: Omit<UseInfiniteQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useInfiniteQuery({
    queryKey: Common.UseDagServiceGetDagTagsKeyFn({ limit, orderBy, tagNamePattern }, queryKey),
    queryFn: ({ pageParam }) =>
      DagService.getDagTags({ limit, offset: pageParam as number, orderBy, tagNamePattern }) as TData,
    initialPageParam: 0,
    getNextPageParam: (lastPage, _allPages, lastPageParam, _allPageParams) =>
      (lastPageParam as number) < (lastPage as Common.DagServiceGetDagTagsDefaultResponse).total_entries
        ? (lastPage as Common.DagServiceGetDagTagsDefaultResponse).tags.length + (lastPageParam as number)
        : undefined,
    getPreviousPageParam: (firstPage, _allPages, firstPageParam, _allPageParams) =>
      (firstPageParam as number) > 0
        ? -(firstPage as Common.DagServiceGetDagTagsDefaultResponse).tags.length + (firstPageParam as number)
        : undefined,
    ...options,
  });
