// generated with @7nohe/openapi-react-query-codegen@1.6.0
import { UseQueryOptions, useSuspenseQuery } from "@tanstack/react-query";

import {
  AssetService,
  BackfillService,
  ConnectionService,
  DagRunService,
  DagService,
  DagSourceService,
  DagStatsService,
  DagWarningService,
  DagsService,
  DashboardService,
  EventLogService,
  ImportErrorService,
  MonitorService,
  PluginService,
  PoolService,
  ProviderService,
  TaskInstanceService,
  VariableService,
  VersionService,
  XcomService,
} from "../requests/services.gen";
import { DagRunState, DagWarningType } from "../requests/types.gen";
import * as Common from "./common";

/**
 * Next Run Assets
 * @param data The data for the request.
 * @param data.dagId
 * @returns unknown Successful Response
 * @throws ApiError
 */
export const useAssetServiceNextRunAssetsSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseAssetServiceNextRunAssetsKeyFn({ dagId }, queryKey),
    queryFn: () => AssetService.nextRunAssets({ dagId }) as TData,
    ...options,
  });
/**
 * Get Assets
 * Get assets.
 * @param data The data for the request.
 * @param data.limit
 * @param data.offset
 * @param data.uriPattern
 * @param data.dagIds
 * @param data.orderBy
 * @returns AssetCollectionResponse Successful Response
 * @throws ApiError
 */
export const useAssetServiceGetAssetsSuspense = <
  TData = Common.AssetServiceGetAssetsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagIds,
    limit,
    offset,
    orderBy,
    uriPattern,
  }: {
    dagIds?: string[];
    limit?: number;
    offset?: number;
    orderBy?: string;
    uriPattern?: string;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseAssetServiceGetAssetsKeyFn(
      { dagIds, limit, offset, orderBy, uriPattern },
      queryKey,
    ),
    queryFn: () =>
      AssetService.getAssets({
        dagIds,
        limit,
        offset,
        orderBy,
        uriPattern,
      }) as TData,
    ...options,
  });
/**
 * Get Asset
 * Get an asset.
 * @param data The data for the request.
 * @param data.uri
 * @returns AssetResponse Successful Response
 * @throws ApiError
 */
export const useAssetServiceGetAssetSuspense = <
  TData = Common.AssetServiceGetAssetDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    uri,
  }: {
    uri: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseAssetServiceGetAssetKeyFn({ uri }, queryKey),
    queryFn: () => AssetService.getAsset({ uri }) as TData,
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
export const useDashboardServiceHistoricalMetricsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useDagsServiceRecentDagRunsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
 * List Backfills
 * @param data The data for the request.
 * @param data.dagId
 * @param data.limit
 * @param data.offset
 * @param data.orderBy
 * @returns unknown Successful Response
 * @throws ApiError
 */
export const useBackfillServiceListBackfillsSuspense = <
  TData = Common.BackfillServiceListBackfillsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    limit,
    offset,
    orderBy,
  }: {
    dagId: string;
    limit?: number;
    offset?: number;
    orderBy?: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseBackfillServiceListBackfillsKeyFn(
      { dagId, limit, offset, orderBy },
      queryKey,
    ),
    queryFn: () =>
      BackfillService.listBackfills({ dagId, limit, offset, orderBy }) as TData,
    ...options,
  });
/**
 * Get Backfill
 * @param data The data for the request.
 * @param data.backfillId
 * @returns unknown Successful Response
 * @throws ApiError
 */
export const useBackfillServiceGetBackfillSuspense = <
  TData = Common.BackfillServiceGetBackfillDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    backfillId,
  }: {
    backfillId: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseBackfillServiceGetBackfillKeyFn(
      { backfillId },
      queryKey,
    ),
    queryFn: () => BackfillService.getBackfill({ backfillId }) as TData,
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
export const useDagServiceGetDagsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useDagServiceGetDagTagsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useDagServiceGetDagSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useDagServiceGetDagDetailsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useConnectionServiceGetConnectionSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useConnectionServiceGetConnectionsSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseConnectionServiceGetConnectionsKeyFn(
      { limit, offset, orderBy },
      queryKey,
    ),
    queryFn: () =>
      ConnectionService.getConnections({ limit, offset, orderBy }) as TData,
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
export const useDagRunServiceGetDagRunSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseDagRunServiceGetDagRunKeyFn(
      { dagId, dagRunId },
      queryKey,
    ),
    queryFn: () => DagRunService.getDagRun({ dagId, dagRunId }) as TData,
    ...options,
  });
/**
 * Get Dag Source
 * Get source code using file token.
 * @param data The data for the request.
 * @param data.fileToken
 * @param data.accept
 * @returns DAGSourceResponse Successful Response
 * @throws ApiError
 */
export const useDagSourceServiceGetDagSourceSuspense = <
  TData = Common.DagSourceServiceGetDagSourceDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    accept,
    fileToken,
  }: {
    accept?: string;
    fileToken: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseDagSourceServiceGetDagSourceKeyFn(
      { accept, fileToken },
      queryKey,
    ),
    queryFn: () =>
      DagSourceService.getDagSource({ accept, fileToken }) as TData,
    ...options,
  });
/**
 * Get Event Log
 * @param data The data for the request.
 * @param data.eventLogId
 * @returns EventLogResponse Successful Response
 * @throws ApiError
 */
export const useEventLogServiceGetEventLogSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseEventLogServiceGetEventLogKeyFn(
      { eventLogId },
      queryKey,
    ),
    queryFn: () => EventLogService.getEventLog({ eventLogId }) as TData,
    ...options,
  });
/**
 * Get Event Logs
 * Get all Event Logs.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.taskId
 * @param data.runId
 * @param data.mapIndex
 * @param data.tryNumber
 * @param data.owner
 * @param data.event
 * @param data.excludedEvents
 * @param data.includedEvents
 * @param data.before
 * @param data.after
 * @param data.limit
 * @param data.offset
 * @param data.orderBy
 * @returns EventLogCollectionResponse Successful Response
 * @throws ApiError
 */
export const useEventLogServiceGetEventLogsSuspense = <
  TData = Common.EventLogServiceGetEventLogsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    after,
    before,
    dagId,
    event,
    excludedEvents,
    includedEvents,
    limit,
    mapIndex,
    offset,
    orderBy,
    owner,
    runId,
    taskId,
    tryNumber,
  }: {
    after?: string;
    before?: string;
    dagId?: string;
    event?: string;
    excludedEvents?: string[];
    includedEvents?: string[];
    limit?: number;
    mapIndex?: number;
    offset?: number;
    orderBy?: string;
    owner?: string;
    runId?: string;
    taskId?: string;
    tryNumber?: number;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseEventLogServiceGetEventLogsKeyFn(
      {
        after,
        before,
        dagId,
        event,
        excludedEvents,
        includedEvents,
        limit,
        mapIndex,
        offset,
        orderBy,
        owner,
        runId,
        taskId,
        tryNumber,
      },
      queryKey,
    ),
    queryFn: () =>
      EventLogService.getEventLogs({
        after,
        before,
        dagId,
        event,
        excludedEvents,
        includedEvents,
        limit,
        mapIndex,
        offset,
        orderBy,
        owner,
        runId,
        taskId,
        tryNumber,
      }) as TData,
    ...options,
  });
/**
 * Get Import Error
 * Get an import error.
 * @param data The data for the request.
 * @param data.importErrorId
 * @returns ImportErrorResponse Successful Response
 * @throws ApiError
 */
export const useImportErrorServiceGetImportErrorSuspense = <
  TData = Common.ImportErrorServiceGetImportErrorDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    importErrorId,
  }: {
    importErrorId: number;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseImportErrorServiceGetImportErrorKeyFn(
      { importErrorId },
      queryKey,
    ),
    queryFn: () =>
      ImportErrorService.getImportError({ importErrorId }) as TData,
    ...options,
  });
/**
 * Get Import Errors
 * Get all import errors.
 * @param data The data for the request.
 * @param data.limit
 * @param data.offset
 * @param data.orderBy
 * @returns ImportErrorCollectionResponse Successful Response
 * @throws ApiError
 */
export const useImportErrorServiceGetImportErrorsSuspense = <
  TData = Common.ImportErrorServiceGetImportErrorsDefaultResponse,
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseImportErrorServiceGetImportErrorsKeyFn(
      { limit, offset, orderBy },
      queryKey,
    ),
    queryFn: () =>
      ImportErrorService.getImportErrors({ limit, offset, orderBy }) as TData,
    ...options,
  });
/**
 * Get Health
 * @returns HealthInfoSchema Successful Response
 * @throws ApiError
 */
export const useMonitorServiceGetHealthSuspense = <
  TData = Common.MonitorServiceGetHealthDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseMonitorServiceGetHealthKeyFn(queryKey),
    queryFn: () => MonitorService.getHealth() as TData,
    ...options,
  });
/**
 * List Dag Warnings
 * Get a list of DAG warnings.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.warningType
 * @param data.limit
 * @param data.offset
 * @param data.orderBy
 * @returns DAGWarningCollectionResponse Successful Response
 * @throws ApiError
 */
export const useDagWarningServiceListDagWarningsSuspense = <
  TData = Common.DagWarningServiceListDagWarningsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    limit,
    offset,
    orderBy,
    warningType,
  }: {
    dagId?: string;
    limit?: number;
    offset?: number;
    orderBy?: string;
    warningType?: DagWarningType;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseDagWarningServiceListDagWarningsKeyFn(
      { dagId, limit, offset, orderBy, warningType },
      queryKey,
    ),
    queryFn: () =>
      DagWarningService.listDagWarnings({
        dagId,
        limit,
        offset,
        orderBy,
        warningType,
      }) as TData,
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
export const usePluginServiceGetPluginsSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UsePluginServiceGetPluginsKeyFn(
      { limit, offset },
      queryKey,
    ),
    queryFn: () => PluginService.getPlugins({ limit, offset }) as TData,
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
export const usePoolServiceGetPoolSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const usePoolServiceGetPoolsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useProviderServiceGetProvidersSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseProviderServiceGetProvidersKeyFn(
      { limit, offset },
      queryKey,
    ),
    queryFn: () => ProviderService.getProviders({ limit, offset }) as TData,
    ...options,
  });
/**
 * Get Task Instance
 * Get task instance.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @param data.taskId
 * @returns TaskInstanceResponse Successful Response
 * @throws ApiError
 */
export const useTaskInstanceServiceGetTaskInstanceSuspense = <
  TData = Common.TaskInstanceServiceGetTaskInstanceDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    dagRunId,
    taskId,
  }: {
    dagId: string;
    dagRunId: string;
    taskId: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseTaskInstanceServiceGetTaskInstanceKeyFn(
      { dagId, dagRunId, taskId },
      queryKey,
    ),
    queryFn: () =>
      TaskInstanceService.getTaskInstance({ dagId, dagRunId, taskId }) as TData,
    ...options,
  });
/**
 * Get Mapped Task Instances
 * Get list of mapped task instances.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @param data.taskId
 * @param data.logicalDateGte
 * @param data.logicalDateLte
 * @param data.startDateGte
 * @param data.startDateLte
 * @param data.endDateGte
 * @param data.endDateLte
 * @param data.updatedAtGte
 * @param data.updatedAtLte
 * @param data.durationGte
 * @param data.durationLte
 * @param data.state
 * @param data.pool
 * @param data.queue
 * @param data.executor
 * @param data.limit
 * @param data.offset
 * @param data.orderBy
 * @returns TaskInstanceCollectionResponse Successful Response
 * @throws ApiError
 */
export const useTaskInstanceServiceGetMappedTaskInstancesSuspense = <
  TData = Common.TaskInstanceServiceGetMappedTaskInstancesDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    dagRunId,
    durationGte,
    durationLte,
    endDateGte,
    endDateLte,
    executor,
    limit,
    logicalDateGte,
    logicalDateLte,
    offset,
    orderBy,
    pool,
    queue,
    startDateGte,
    startDateLte,
    state,
    taskId,
    updatedAtGte,
    updatedAtLte,
  }: {
    dagId: string;
    dagRunId: string;
    durationGte?: number;
    durationLte?: number;
    endDateGte?: string;
    endDateLte?: string;
    executor?: string[];
    limit?: number;
    logicalDateGte?: string;
    logicalDateLte?: string;
    offset?: number;
    orderBy?: string;
    pool?: string[];
    queue?: string[];
    startDateGte?: string;
    startDateLte?: string;
    state?: string[];
    taskId: string;
    updatedAtGte?: string;
    updatedAtLte?: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstancesKeyFn(
      {
        dagId,
        dagRunId,
        durationGte,
        durationLte,
        endDateGte,
        endDateLte,
        executor,
        limit,
        logicalDateGte,
        logicalDateLte,
        offset,
        orderBy,
        pool,
        queue,
        startDateGte,
        startDateLte,
        state,
        taskId,
        updatedAtGte,
        updatedAtLte,
      },
      queryKey,
    ),
    queryFn: () =>
      TaskInstanceService.getMappedTaskInstances({
        dagId,
        dagRunId,
        durationGte,
        durationLte,
        endDateGte,
        endDateLte,
        executor,
        limit,
        logicalDateGte,
        logicalDateLte,
        offset,
        orderBy,
        pool,
        queue,
        startDateGte,
        startDateLte,
        state,
        taskId,
        updatedAtGte,
        updatedAtLte,
      }) as TData,
    ...options,
  });
/**
 * Get Task Instance Dependencies
 * Get dependencies blocking task from getting scheduled.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @param data.taskId
 * @param data.mapIndex
 * @returns TaskDependencyCollectionResponse Successful Response
 * @throws ApiError
 */
export const useTaskInstanceServiceGetTaskInstanceDependenciesSuspense = <
  TData = Common.TaskInstanceServiceGetTaskInstanceDependenciesDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    dagRunId,
    mapIndex,
    taskId,
  }: {
    dagId: string;
    dagRunId: string;
    mapIndex: number;
    taskId: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseTaskInstanceServiceGetTaskInstanceDependenciesKeyFn(
      { dagId, dagRunId, mapIndex, taskId },
      queryKey,
    ),
    queryFn: () =>
      TaskInstanceService.getTaskInstanceDependencies({
        dagId,
        dagRunId,
        mapIndex,
        taskId,
      }) as TData,
    ...options,
  });
/**
 * Get Task Instance Dependencies
 * Get dependencies blocking task from getting scheduled.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @param data.taskId
 * @param data.mapIndex
 * @returns TaskDependencyCollectionResponse Successful Response
 * @throws ApiError
 */
export const useTaskInstanceServiceGetTaskInstanceDependencies1Suspense = <
  TData = Common.TaskInstanceServiceGetTaskInstanceDependencies1DefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    dagRunId,
    mapIndex,
    taskId,
  }: {
    dagId: string;
    dagRunId: string;
    mapIndex?: number;
    taskId: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseTaskInstanceServiceGetTaskInstanceDependencies1KeyFn(
      { dagId, dagRunId, mapIndex, taskId },
      queryKey,
    ),
    queryFn: () =>
      TaskInstanceService.getTaskInstanceDependencies1({
        dagId,
        dagRunId,
        mapIndex,
        taskId,
      }) as TData,
    ...options,
  });
/**
 * Get Mapped Task Instance
 * Get task instance.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @param data.taskId
 * @param data.mapIndex
 * @returns TaskInstanceResponse Successful Response
 * @throws ApiError
 */
export const useTaskInstanceServiceGetMappedTaskInstanceSuspense = <
  TData = Common.TaskInstanceServiceGetMappedTaskInstanceDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    dagRunId,
    mapIndex,
    taskId,
  }: {
    dagId: string;
    dagRunId: string;
    mapIndex: number;
    taskId: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstanceKeyFn(
      { dagId, dagRunId, mapIndex, taskId },
      queryKey,
    ),
    queryFn: () =>
      TaskInstanceService.getMappedTaskInstance({
        dagId,
        dagRunId,
        mapIndex,
        taskId,
      }) as TData,
    ...options,
  });
/**
 * Get Task Instances
 * Get list of task instances.
 *
 * This endpoint allows specifying `~` as the dag_id, dag_run_id to retrieve Task Instances for all DAGs
 * and DAG runs.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @param data.logicalDateGte
 * @param data.logicalDateLte
 * @param data.startDateGte
 * @param data.startDateLte
 * @param data.endDateGte
 * @param data.endDateLte
 * @param data.updatedAtGte
 * @param data.updatedAtLte
 * @param data.durationGte
 * @param data.durationLte
 * @param data.state
 * @param data.pool
 * @param data.queue
 * @param data.executor
 * @param data.limit
 * @param data.offset
 * @param data.orderBy
 * @returns TaskInstanceCollectionResponse Successful Response
 * @throws ApiError
 */
export const useTaskInstanceServiceGetTaskInstancesSuspense = <
  TData = Common.TaskInstanceServiceGetTaskInstancesDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    dagRunId,
    durationGte,
    durationLte,
    endDateGte,
    endDateLte,
    executor,
    limit,
    logicalDateGte,
    logicalDateLte,
    offset,
    orderBy,
    pool,
    queue,
    startDateGte,
    startDateLte,
    state,
    updatedAtGte,
    updatedAtLte,
  }: {
    dagId: string;
    dagRunId: string;
    durationGte?: number;
    durationLte?: number;
    endDateGte?: string;
    endDateLte?: string;
    executor?: string[];
    limit?: number;
    logicalDateGte?: string;
    logicalDateLte?: string;
    offset?: number;
    orderBy?: string;
    pool?: string[];
    queue?: string[];
    startDateGte?: string;
    startDateLte?: string;
    state?: string[];
    updatedAtGte?: string;
    updatedAtLte?: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseTaskInstanceServiceGetTaskInstancesKeyFn(
      {
        dagId,
        dagRunId,
        durationGte,
        durationLte,
        endDateGte,
        endDateLte,
        executor,
        limit,
        logicalDateGte,
        logicalDateLte,
        offset,
        orderBy,
        pool,
        queue,
        startDateGte,
        startDateLte,
        state,
        updatedAtGte,
        updatedAtLte,
      },
      queryKey,
    ),
    queryFn: () =>
      TaskInstanceService.getTaskInstances({
        dagId,
        dagRunId,
        durationGte,
        durationLte,
        endDateGte,
        endDateLte,
        executor,
        limit,
        logicalDateGte,
        logicalDateLte,
        offset,
        orderBy,
        pool,
        queue,
        startDateGte,
        startDateLte,
        state,
        updatedAtGte,
        updatedAtLte,
      }) as TData,
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
export const useVariableServiceGetVariableSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useVariableServiceGetVariablesSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseVariableServiceGetVariablesKeyFn(
      { limit, offset, orderBy },
      queryKey,
    ),
    queryFn: () =>
      VariableService.getVariables({ limit, offset, orderBy }) as TData,
    ...options,
  });
/**
 * Get Version
 * Get version information.
 * @returns VersionInfo Successful Response
 * @throws ApiError
 */
export const useVersionServiceGetVersionSuspense = <
  TData = Common.VersionServiceGetVersionDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseVersionServiceGetVersionKeyFn(queryKey),
    queryFn: () => VersionService.getVersion() as TData,
    ...options,
  });
/**
 * Get Dag Stats
 * Get Dag statistics.
 * @param data The data for the request.
 * @param data.dagIds
 * @returns DagStatsCollectionResponse Successful Response
 * @throws ApiError
 */
export const useDagStatsServiceGetDagStatsSuspense = <
  TData = Common.DagStatsServiceGetDagStatsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagIds,
  }: {
    dagIds?: string[];
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseDagStatsServiceGetDagStatsKeyFn({ dagIds }, queryKey),
    queryFn: () => DagStatsService.getDagStats({ dagIds }) as TData,
    ...options,
  });
/**
 * Get Xcom Entry
 * Get an XCom entry.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.taskId
 * @param data.dagRunId
 * @param data.xcomKey
 * @param data.mapIndex
 * @param data.deserialize
 * @param data.stringify
 * @returns unknown Successful Response
 * @throws ApiError
 */
export const useXcomServiceGetXcomEntrySuspense = <
  TData = Common.XcomServiceGetXcomEntryDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    dagRunId,
    deserialize,
    mapIndex,
    stringify,
    taskId,
    xcomKey,
  }: {
    dagId: string;
    dagRunId: string;
    deserialize?: boolean;
    mapIndex?: number;
    stringify?: boolean;
    taskId: string;
    xcomKey: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseXcomServiceGetXcomEntryKeyFn(
      { dagId, dagRunId, deserialize, mapIndex, stringify, taskId, xcomKey },
      queryKey,
    ),
    queryFn: () =>
      XcomService.getXcomEntry({
        dagId,
        dagRunId,
        deserialize,
        mapIndex,
        stringify,
        taskId,
        xcomKey,
      }) as TData,
    ...options,
  });
