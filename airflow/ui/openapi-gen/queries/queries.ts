// generated with @7nohe/openapi-react-query-codegen@1.6.0
import {
  useMutation,
  UseMutationOptions,
  useQuery,
  UseQueryOptions,
} from "@tanstack/react-query";

import {
  AssetService,
  ConnectionService,
  DagService,
} from "../requests/services.gen";
import { DAGPatchBody, DagRunState } from "../requests/types.gen";
import * as Common from "./common";

/**
 * Next Run Assets
 * @param data The data for the request.
 * @param data.dagId
 * @returns unknown Successful Response
 * @throws ApiError
 */
export const useAssetServiceNextRunAssetsUiNextRunDatasetsDagIdGet = <
  TData = Common.AssetServiceNextRunAssetsUiNextRunDatasetsDagIdGetDefaultResponse,
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
    queryKey: Common.UseAssetServiceNextRunAssetsUiNextRunDatasetsDagIdGetKeyFn(
      { dagId },
      queryKey,
    ),
    queryFn: () =>
      AssetService.nextRunAssetsUiNextRunDatasetsDagIdGet({ dagId }) as TData,
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
 * @param data.lastDagRunState
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
    lastDagRunState,
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
    lastDagRunState?: DagRunState;
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
        lastDagRunState,
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
        lastDagRunState,
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
 * Patch Dags
 * Patch multiple DAGs.
 * @param data The data for the request.
 * @param data.requestBody
 * @param data.updateMask
 * @param data.limit
 * @param data.offset
 * @param data.tags
 * @param data.owners
 * @param data.dagIdPattern
 * @param data.onlyActive
 * @param data.paused
 * @param data.lastDagRunState
 * @returns DAGCollectionResponse Successful Response
 * @throws ApiError
 */
export const useDagServicePatchDagsPublicDagsPatch = <
  TData = Common.DagServicePatchDagsPublicDagsPatchMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        dagIdPattern?: string;
        lastDagRunState?: DagRunState;
        limit?: number;
        offset?: number;
        onlyActive?: boolean;
        owners?: string[];
        paused?: boolean;
        requestBody: DAGPatchBody;
        tags?: string[];
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
      dagIdPattern?: string;
      lastDagRunState?: DagRunState;
      limit?: number;
      offset?: number;
      onlyActive?: boolean;
      owners?: string[];
      paused?: boolean;
      requestBody: DAGPatchBody;
      tags?: string[];
      updateMask?: string[];
    },
    TContext
  >({
    mutationFn: ({
      dagIdPattern,
      lastDagRunState,
      limit,
      offset,
      onlyActive,
      owners,
      paused,
      requestBody,
      tags,
      updateMask,
    }) =>
      DagService.patchDagsPublicDagsPatch({
        dagIdPattern,
        lastDagRunState,
        limit,
        offset,
        onlyActive,
        owners,
        paused,
        requestBody,
        tags,
        updateMask,
      }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Patch Dag
 * Patch the specific DAG.
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
/**
 * Delete Connection
 * Delete a connection entry.
 * @param data The data for the request.
 * @param data.connectionId
 * @returns void Successful Response
 * @throws ApiError
 */
export const useConnectionServiceDeleteConnectionPublicConnectionsConnectionIdDelete =
  <
    TData = Common.ConnectionServiceDeleteConnectionPublicConnectionsConnectionIdDeleteMutationResult,
    TError = unknown,
    TContext = unknown,
  >(
    options?: Omit<
      UseMutationOptions<
        TData,
        TError,
        {
          connectionId: string;
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
        connectionId: string;
      },
      TContext
    >({
      mutationFn: ({ connectionId }) =>
        ConnectionService.deleteConnectionPublicConnectionsConnectionIdDelete({
          connectionId,
        }) as unknown as Promise<TData>,
      ...options,
    });
