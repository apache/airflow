// generated with @7nohe/openapi-react-query-codegen@1.6.0
import {
  UseMutationOptions,
  UseQueryOptions,
  useMutation,
  useQuery,
} from "@tanstack/react-query";

import {
  AssetService,
  ConnectionService,
  DagRunService,
  DagService,
  DagsService,
  DashboardService,
  EventLogService,
  MonitorService,
  PluginService,
  PoolService,
  ProviderService,
  VariableService,
  VersionService,
} from "../requests/services.gen";
import {
  DAGPatchBody,
  DagRunState,
  PoolPatchBody,
  PoolPostBody,
  VariableBody,
} from "../requests/types.gen";
import * as Common from "./common";

/**
 * Next Run Assets
 * @param data The data for the request.
 * @param data.dagId
 * @returns unknown Successful Response
 * @throws ApiError
 */
export const useAssetServiceNextRunAssets = <
  TData = Common.AssetServiceNextRunAssetsDefaultResponse,
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
    queryKey: Common.UseAssetServiceNextRunAssetsKeyFn({ dagId }, queryKey),
    queryFn: () => AssetService.nextRunAssets({ dagId }) as TData,
    ...options,
  });
/**
 * Historical Metrics
 * Return cluster activity historical metrics.
 * @param data The data for the request.
 * @param data.startDate
 * @param data.endDate
 * @returns HistoricalMetricDataResponse Successful Response
 * @throws ApiError
 */
export const useDashboardServiceHistoricalMetrics = <
  TData = Common.DashboardServiceHistoricalMetricsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    endDate,
    startDate,
  }: {
    endDate: string;
    startDate: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDashboardServiceHistoricalMetricsKeyFn(
      { endDate, startDate },
      queryKey,
    ),
    queryFn: () =>
      DashboardService.historicalMetrics({ endDate, startDate }) as TData,
    ...options,
  });
/**
 * Recent Dag Runs
 * Get recent DAG runs.
 * @param data The data for the request.
 * @param data.dagRunsLimit
 * @param data.limit
 * @param data.offset
 * @param data.tags
 * @param data.owners
 * @param data.dagIdPattern
 * @param data.dagDisplayNamePattern
 * @param data.onlyActive
 * @param data.paused
 * @param data.lastDagRunState
 * @returns DAGWithLatestDagRunsCollectionResponse Successful Response
 * @throws ApiError
 */
export const useDagsServiceRecentDagRuns = <
  TData = Common.DagsServiceRecentDagRunsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagDisplayNamePattern,
    dagIdPattern,
    dagRunsLimit,
    lastDagRunState,
    limit,
    offset,
    onlyActive,
    owners,
    paused,
    tags,
  }: {
    dagDisplayNamePattern?: string;
    dagIdPattern?: string;
    dagRunsLimit?: number;
    lastDagRunState?: DagRunState;
    limit?: number;
    offset?: number;
    onlyActive?: boolean;
    owners?: string[];
    paused?: boolean;
    tags?: string[];
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDagsServiceRecentDagRunsKeyFn(
      {
        dagDisplayNamePattern,
        dagIdPattern,
        dagRunsLimit,
        lastDagRunState,
        limit,
        offset,
        onlyActive,
        owners,
        paused,
        tags,
      },
      queryKey,
    ),
    queryFn: () =>
      DagsService.recentDagRuns({
        dagDisplayNamePattern,
        dagIdPattern,
        dagRunsLimit,
        lastDagRunState,
        limit,
        offset,
        onlyActive,
        owners,
        paused,
        tags,
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
 * @param data.lastDagRunState
 * @param data.orderBy
 * @returns DAGCollectionResponse Successful Response
 * @throws ApiError
 */
export const useDagServiceGetDags = <
  TData = Common.DagServiceGetDagsDefaultResponse,
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
    queryKey: Common.UseDagServiceGetDagsKeyFn(
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
      DagService.getDags({
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
 * Get Dag Tags
 * Get all DAG tags.
 * @param data The data for the request.
 * @param data.limit
 * @param data.offset
 * @param data.orderBy
 * @param data.tagNamePattern
 * @returns DAGTagCollectionResponse Successful Response
 * @throws ApiError
 */
export const useDagServiceGetDagTags = <
  TData = Common.DagServiceGetDagTagsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    limit,
    offset,
    orderBy,
    tagNamePattern,
  }: {
    limit?: number;
    offset?: number;
    orderBy?: string;
    tagNamePattern?: string;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDagServiceGetDagTagsKeyFn(
      { limit, offset, orderBy, tagNamePattern },
      queryKey,
    ),
    queryFn: () =>
      DagService.getDagTags({
        limit,
        offset,
        orderBy,
        tagNamePattern,
      }) as TData,
    ...options,
  });
/**
 * Get Dag
 * Get basic information about a DAG.
 * @param data The data for the request.
 * @param data.dagId
 * @returns DAGResponse Successful Response
 * @throws ApiError
 */
export const useDagServiceGetDag = <
  TData = Common.DagServiceGetDagDefaultResponse,
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
    queryKey: Common.UseDagServiceGetDagKeyFn({ dagId }, queryKey),
    queryFn: () => DagService.getDag({ dagId }) as TData,
    ...options,
  });
/**
 * Get Dag Details
 * Get details of DAG.
 * @param data The data for the request.
 * @param data.dagId
 * @returns DAGDetailsResponse Successful Response
 * @throws ApiError
 */
export const useDagServiceGetDagDetails = <
  TData = Common.DagServiceGetDagDetailsDefaultResponse,
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
    queryKey: Common.UseDagServiceGetDagDetailsKeyFn({ dagId }, queryKey),
    queryFn: () => DagService.getDagDetails({ dagId }) as TData,
    ...options,
  });
/**
 * Get Connection
 * Get a connection entry.
 * @param data The data for the request.
 * @param data.connectionId
 * @returns ConnectionResponse Successful Response
 * @throws ApiError
 */
export const useConnectionServiceGetConnection = <
  TData = Common.ConnectionServiceGetConnectionDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    connectionId,
  }: {
    connectionId: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseConnectionServiceGetConnectionKeyFn(
      { connectionId },
      queryKey,
    ),
    queryFn: () => ConnectionService.getConnection({ connectionId }) as TData,
    ...options,
  });
/**
 * Get Connections
 * Get all connection entries.
 * @param data The data for the request.
 * @param data.limit
 * @param data.offset
 * @param data.orderBy
 * @returns ConnectionCollectionResponse Successful Response
 * @throws ApiError
 */
export const useConnectionServiceGetConnections = <
  TData = Common.ConnectionServiceGetConnectionsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    limit,
    offset,
    orderBy,
  }: {
    limit?: number;
    offset?: number;
    orderBy?: string;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseConnectionServiceGetConnectionsKeyFn(
      { limit, offset, orderBy },
      queryKey,
    ),
    queryFn: () =>
      ConnectionService.getConnections({ limit, offset, orderBy }) as TData,
    ...options,
  });
/**
 * Get Variable
 * Get a variable entry.
 * @param data The data for the request.
 * @param data.variableKey
 * @returns VariableResponse Successful Response
 * @throws ApiError
 */
export const useVariableServiceGetVariable = <
  TData = Common.VariableServiceGetVariableDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    variableKey,
  }: {
    variableKey: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseVariableServiceGetVariableKeyFn(
      { variableKey },
      queryKey,
    ),
    queryFn: () => VariableService.getVariable({ variableKey }) as TData,
    ...options,
  });
/**
 * Get Variables
 * Get all Variables entries.
 * @param data The data for the request.
 * @param data.limit
 * @param data.offset
 * @param data.orderBy
 * @returns VariableCollectionResponse Successful Response
 * @throws ApiError
 */
export const useVariableServiceGetVariables = <
  TData = Common.VariableServiceGetVariablesDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    limit,
    offset,
    orderBy,
  }: {
    limit?: number;
    offset?: number;
    orderBy?: string;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseVariableServiceGetVariablesKeyFn(
      { limit, offset, orderBy },
      queryKey,
    ),
    queryFn: () =>
      VariableService.getVariables({ limit, offset, orderBy }) as TData,
    ...options,
  });
/**
 * Get Dag Run
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @returns DAGRunResponse Successful Response
 * @throws ApiError
 */
export const useDagRunServiceGetDagRun = <
  TData = Common.DagRunServiceGetDagRunDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    dagRunId,
  }: {
    dagId: string;
    dagRunId: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDagRunServiceGetDagRunKeyFn(
      { dagId, dagRunId },
      queryKey,
    ),
    queryFn: () => DagRunService.getDagRun({ dagId, dagRunId }) as TData,
    ...options,
  });
/**
 * Get Health
 * @returns HealthInfoSchema Successful Response
 * @throws ApiError
 */
export const useMonitorServiceGetHealth = <
  TData = Common.MonitorServiceGetHealthDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseMonitorServiceGetHealthKeyFn(queryKey),
    queryFn: () => MonitorService.getHealth() as TData,
    ...options,
  });
/**
 * Get Pool
 * Get a pool.
 * @param data The data for the request.
 * @param data.poolName
 * @returns PoolResponse Successful Response
 * @throws ApiError
 */
export const usePoolServiceGetPool = <
  TData = Common.PoolServiceGetPoolDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    poolName,
  }: {
    poolName: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UsePoolServiceGetPoolKeyFn({ poolName }, queryKey),
    queryFn: () => PoolService.getPool({ poolName }) as TData,
    ...options,
  });
/**
 * Get Pools
 * Get all pools entries.
 * @param data The data for the request.
 * @param data.limit
 * @param data.offset
 * @param data.orderBy
 * @returns PoolCollectionResponse Successful Response
 * @throws ApiError
 */
export const usePoolServiceGetPools = <
  TData = Common.PoolServiceGetPoolsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    limit,
    offset,
    orderBy,
  }: {
    limit?: number;
    offset?: number;
    orderBy?: string;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UsePoolServiceGetPoolsKeyFn(
      { limit, offset, orderBy },
      queryKey,
    ),
    queryFn: () => PoolService.getPools({ limit, offset, orderBy }) as TData,
    ...options,
  });
/**
 * Get Providers
 * Get providers.
 * @param data The data for the request.
 * @param data.limit
 * @param data.offset
 * @returns ProviderCollectionResponse Successful Response
 * @throws ApiError
 */
export const useProviderServiceGetProviders = <
  TData = Common.ProviderServiceGetProvidersDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    limit,
    offset,
  }: {
    limit?: number;
    offset?: number;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseProviderServiceGetProvidersKeyFn(
      { limit, offset },
      queryKey,
    ),
    queryFn: () => ProviderService.getProviders({ limit, offset }) as TData,
    ...options,
  });
/**
 * Get Plugins
 * @param data The data for the request.
 * @param data.limit
 * @param data.offset
 * @returns PluginCollectionResponse Successful Response
 * @throws ApiError
 */
export const usePluginServiceGetPlugins = <
  TData = Common.PluginServiceGetPluginsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    limit,
    offset,
  }: {
    limit?: number;
    offset?: number;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UsePluginServiceGetPluginsKeyFn(
      { limit, offset },
      queryKey,
    ),
    queryFn: () => PluginService.getPlugins({ limit, offset }) as TData,
    ...options,
  });
/**
 * Get Version
 * Get version information.
 * @returns VersionInfo Successful Response
 * @throws ApiError
 */
export const useVersionServiceGetVersion = <
  TData = Common.VersionServiceGetVersionDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseVersionServiceGetVersionKeyFn(queryKey),
    queryFn: () => VersionService.getVersion() as TData,
    ...options,
  });
/**
 * Get Event Log
 * @param data The data for the request.
 * @param data.eventLogId
 * @returns EventLogResponse Successful Response
 * @throws ApiError
 */
export const useEventLogServiceGetEventLog = <
  TData = Common.EventLogServiceGetEventLogDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    eventLogId,
  }: {
    eventLogId: number;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseEventLogServiceGetEventLogKeyFn(
      { eventLogId },
      queryKey,
    ),
    queryFn: () => EventLogService.getEventLog({ eventLogId }) as TData,
    ...options,
  });
/**
 * Post Variable
 * Create a variable.
 * @param data The data for the request.
 * @param data.requestBody
 * @returns VariableResponse Successful Response
 * @throws ApiError
 */
export const useVariableServicePostVariable = <
  TData = Common.VariableServicePostVariableMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        requestBody: VariableBody;
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
      requestBody: VariableBody;
    },
    TContext
  >({
    mutationFn: ({ requestBody }) =>
      VariableService.postVariable({
        requestBody,
      }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Post Pool
 * Create a Pool.
 * @param data The data for the request.
 * @param data.requestBody
 * @returns PoolResponse Successful Response
 * @throws ApiError
 */
export const usePoolServicePostPool = <
  TData = Common.PoolServicePostPoolMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        requestBody: PoolPostBody;
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
      requestBody: PoolPostBody;
    },
    TContext
  >({
    mutationFn: ({ requestBody }) =>
      PoolService.postPool({ requestBody }) as unknown as Promise<TData>,
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
export const useDagServicePatchDags = <
  TData = Common.DagServicePatchDagsMutationResult,
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
      DagService.patchDags({
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
export const useDagServicePatchDag = <
  TData = Common.DagServicePatchDagMutationResult,
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
      DagService.patchDag({
        dagId,
        requestBody,
        updateMask,
      }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Patch Variable
 * Update a variable by key.
 * @param data The data for the request.
 * @param data.variableKey
 * @param data.requestBody
 * @param data.updateMask
 * @returns VariableResponse Successful Response
 * @throws ApiError
 */
export const useVariableServicePatchVariable = <
  TData = Common.VariableServicePatchVariableMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        requestBody: VariableBody;
        updateMask?: string[];
        variableKey: string;
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
      requestBody: VariableBody;
      updateMask?: string[];
      variableKey: string;
    },
    TContext
  >({
    mutationFn: ({ requestBody, updateMask, variableKey }) =>
      VariableService.patchVariable({
        requestBody,
        updateMask,
        variableKey,
      }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Patch Pool
 * Update a Pool.
 * @param data The data for the request.
 * @param data.poolName
 * @param data.requestBody
 * @param data.updateMask
 * @returns PoolResponse Successful Response
 * @throws ApiError
 */
export const usePoolServicePatchPool = <
  TData = Common.PoolServicePatchPoolMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        poolName: string;
        requestBody: PoolPatchBody;
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
      poolName: string;
      requestBody: PoolPatchBody;
      updateMask?: string[];
    },
    TContext
  >({
    mutationFn: ({ poolName, requestBody, updateMask }) =>
      PoolService.patchPool({
        poolName,
        requestBody,
        updateMask,
      }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Delete Dag
 * Delete the specific DAG.
 * @param data The data for the request.
 * @param data.dagId
 * @returns unknown Successful Response
 * @throws ApiError
 */
export const useDagServiceDeleteDag = <
  TData = Common.DagServiceDeleteDagMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        dagId: string;
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
    },
    TContext
  >({
    mutationFn: ({ dagId }) =>
      DagService.deleteDag({ dagId }) as unknown as Promise<TData>,
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
export const useConnectionServiceDeleteConnection = <
  TData = Common.ConnectionServiceDeleteConnectionMutationResult,
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
      ConnectionService.deleteConnection({
        connectionId,
      }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Delete Variable
 * Delete a variable entry.
 * @param data The data for the request.
 * @param data.variableKey
 * @returns void Successful Response
 * @throws ApiError
 */
export const useVariableServiceDeleteVariable = <
  TData = Common.VariableServiceDeleteVariableMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        variableKey: string;
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
      variableKey: string;
    },
    TContext
  >({
    mutationFn: ({ variableKey }) =>
      VariableService.deleteVariable({
        variableKey,
      }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Delete Dag Run
 * Delete a DAG Run entry.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @returns void Successful Response
 * @throws ApiError
 */
export const useDagRunServiceDeleteDagRun = <
  TData = Common.DagRunServiceDeleteDagRunMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        dagId: string;
        dagRunId: string;
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
      dagRunId: string;
    },
    TContext
  >({
    mutationFn: ({ dagId, dagRunId }) =>
      DagRunService.deleteDagRun({
        dagId,
        dagRunId,
      }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Delete Pool
 * Delete a pool entry.
 * @param data The data for the request.
 * @param data.poolName
 * @returns void Successful Response
 * @throws ApiError
 */
export const usePoolServiceDeletePool = <
  TData = Common.PoolServiceDeletePoolMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        poolName: string;
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
      poolName: string;
    },
    TContext
  >({
    mutationFn: ({ poolName }) =>
      PoolService.deletePool({ poolName }) as unknown as Promise<TData>,
    ...options,
  });
