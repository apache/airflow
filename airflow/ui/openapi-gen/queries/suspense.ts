// generated with @7nohe/openapi-react-query-codegen@1.6.0
import { UseQueryOptions, useSuspenseQuery } from "@tanstack/react-query";

import { DagService, DatasetService } from "../requests/services.gen";
import * as Common from "./common";

/**
 * Next Run Datasets
 * @param data The data for the request.
 * @param data.dagId
 * @returns unknown Successful Response
 * @throws ApiError
 */
export const useDatasetServiceNextRunDatasetsUiNextRunDatasetsDagIdGetSuspense =
  <
    TData = Common.DatasetServiceNextRunDatasetsUiNextRunDatasetsDagIdGetDefaultResponse,
    TError = unknown,
    TQueryKey extends Array<unknown> = unknown[],
  >(
    {
      dagId,
    }: {
      dagId: string;
    },
    queryKey?: TQueryKey,
    options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
  ) =>
    useSuspenseQuery<TData, TError>({
      queryKey:
        Common.UseDatasetServiceNextRunDatasetsUiNextRunDatasetsDagIdGetKeyFn(
          { dagId },
          queryKey,
        ),
      queryFn: () =>
        DatasetService.nextRunDatasetsUiNextRunDatasetsDagIdGet({
          dagId,
        }) as TData,
      ...options,
    });
/**
 * Get Dags
 * Get all DAGs.
 * @param data The data for the request.
 * @param data.limit
 * @param data.offset
 * @param data.tags
 * @param data.dagIdPattern
 * @param data.onlyActive
 * @param data.paused
 * @param data.orderBy
 * @returns DAGCollectionResponse Successful Response
 * @throws ApiError
 */
export const useDagServiceGetDagsPublicDagsGetSuspense = <
  TData = Common.DagServiceGetDagsPublicDagsGetDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagIdPattern,
    limit,
    offset,
    onlyActive,
    orderBy,
    paused,
    tags,
  }: {
    dagIdPattern?: string;
    limit?: number;
    offset?: number;
    onlyActive?: boolean;
    orderBy?: string;
    paused?: boolean;
    tags?: string[];
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseDagServiceGetDagsPublicDagsGetKeyFn(
      { dagIdPattern, limit, offset, onlyActive, orderBy, paused, tags },
      queryKey,
    ),
    queryFn: () =>
      DagService.getDagsPublicDagsGet({
        dagIdPattern,
        limit,
        offset,
        onlyActive,
        orderBy,
        paused,
        tags,
      }) as TData,
    ...options,
  });
