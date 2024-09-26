// generated with @7nohe/openapi-react-query-codegen@1.6.0
import {
  useMutation,
  UseMutationOptions,
  useQuery,
  UseQueryOptions,
} from "@tanstack/react-query";

import { DagService, DatasetService } from "../requests/services.gen";
import { DAGPatchBody } from "../requests/types.gen";
import * as Common from "./common";

/**
 * Next Run Datasets
 * @param data The data for the request.
 * @param data.dagId
 * @returns unknown Successful Response
 * @throws ApiError
 */
export const useDatasetServiceNextRunDatasetsUiNextRunDatasetsDagIdGet = <
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
  useQuery<TData, TError>({
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
 * @param data.owners
 * @param data.dagIdPattern
 * @param data.dagDisplayNamePattern
 * @param data.onlyActive
 * @param data.paused
 * @param data.orderBy
 * @returns DAGCollectionResponse Successful Response
 * @throws ApiError
 */
export const useDagServiceGetDagsPublicDagsGet = <
  TData = Common.DagServiceGetDagsPublicDagsGetDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagDisplayNamePattern,
    dagIdPattern,
    limit,
    offset,
    onlyActive,
    orderBy,
    owners,
    paused,
    tags,
  }: {
    dagDisplayNamePattern?: string;
    dagIdPattern?: string;
    limit?: number;
    offset?: number;
    onlyActive?: boolean;
    orderBy?: string;
    owners?: string[];
    paused?: boolean;
    tags?: string[];
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDagServiceGetDagsPublicDagsGetKeyFn(
      {
        dagDisplayNamePattern,
        dagIdPattern,
        limit,
        offset,
        onlyActive,
        orderBy,
        owners,
        paused,
        tags,
      },
      queryKey,
    ),
    queryFn: () =>
      DagService.getDagsPublicDagsGet({
        dagDisplayNamePattern,
        dagIdPattern,
        limit,
        offset,
        onlyActive,
        orderBy,
        owners,
        paused,
        tags,
      }) as TData,
    ...options,
  });
/**
 * Patch Dag
 * Update the specific DAG.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.requestBody
 * @param data.updateMask
 * @returns DAGResponse Successful Response
 * @throws ApiError
 */
export const useDagServicePatchDagPublicDagsDagIdPatch = <
  TData = Common.DagServicePatchDagPublicDagsDagIdPatchMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        dagId: string;
        requestBody: DAGPatchBody;
        updateMask?: string[];
      },
      TContext
    >,
    "mutationFn"
  >,
) =>
  useMutation<
    TData,
    TError,
    {
      dagId: string;
      requestBody: DAGPatchBody;
      updateMask?: string[];
    },
    TContext
  >({
    mutationFn: ({ dagId, requestBody, updateMask }) =>
      DagService.patchDagPublicDagsDagIdPatch({
        dagId,
        requestBody,
        updateMask,
      }) as unknown as Promise<TData>,
    ...options,
  });
