// generated with @7nohe/openapi-react-query-codegen@1.6.0
import {
  UseMutationOptions,
  UseQueryOptions,
  useMutation,
  useQuery,
} from "@tanstack/react-query";

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
  TaskService,
  VariableService,
  VersionService,
  XcomService,
} from "../requests/services.gen";
import {
  BackfillPostBody,
  ConnectionBody,
  CreateAssetEventsBody,
  DAGPatchBody,
  DAGRunClearBody,
  DAGRunPatchBody,
  DagRunState,
  DagWarningType,
  PoolPatchBody,
  PoolPostBody,
  TaskInstancesBatchBody,
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
export const useAssetServiceGetAssets = <
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
  useQuery<TData, TError>({
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
 * Get Asset Events
 * Get asset events.
 * @param data The data for the request.
 * @param data.limit
 * @param data.offset
 * @param data.orderBy
 * @param data.assetId
 * @param data.sourceDagId
 * @param data.sourceTaskId
 * @param data.sourceRunId
 * @param data.sourceMapIndex
 * @returns AssetEventCollectionResponse Successful Response
 * @throws ApiError
 */
export const useAssetServiceGetAssetEvents = <
  TData = Common.AssetServiceGetAssetEventsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    assetId,
    limit,
    offset,
    orderBy,
    sourceDagId,
    sourceMapIndex,
    sourceRunId,
    sourceTaskId,
  }: {
    assetId?: number;
    limit?: number;
    offset?: number;
    orderBy?: string;
    sourceDagId?: string;
    sourceMapIndex?: number;
    sourceRunId?: string;
    sourceTaskId?: string;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseAssetServiceGetAssetEventsKeyFn(
      {
        assetId,
        limit,
        offset,
        orderBy,
        sourceDagId,
        sourceMapIndex,
        sourceRunId,
        sourceTaskId,
      },
      queryKey,
    ),
    queryFn: () =>
      AssetService.getAssetEvents({
        assetId,
        limit,
        offset,
        orderBy,
        sourceDagId,
        sourceMapIndex,
        sourceRunId,
        sourceTaskId,
      }) as TData,
    ...options,
  });
/**
 * Get Asset Queued Events
 * Get queued asset events for an asset.
 * @param data The data for the request.
 * @param data.uri
 * @param data.before
 * @returns QueuedEventCollectionResponse Successful Response
 * @throws ApiError
 */
export const useAssetServiceGetAssetQueuedEvents = <
  TData = Common.AssetServiceGetAssetQueuedEventsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    before,
    uri,
  }: {
    before?: string;
    uri: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseAssetServiceGetAssetQueuedEventsKeyFn(
      { before, uri },
      queryKey,
    ),
    queryFn: () => AssetService.getAssetQueuedEvents({ before, uri }) as TData,
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
export const useAssetServiceGetAsset = <
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
  useQuery<TData, TError>({
    queryKey: Common.UseAssetServiceGetAssetKeyFn({ uri }, queryKey),
    queryFn: () => AssetService.getAsset({ uri }) as TData,
    ...options,
  });
/**
 * Get Dag Asset Queued Events
 * Get queued asset events for a DAG.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.before
 * @returns QueuedEventCollectionResponse Successful Response
 * @throws ApiError
 */
export const useAssetServiceGetDagAssetQueuedEvents = <
  TData = Common.AssetServiceGetDagAssetQueuedEventsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    before,
    dagId,
  }: {
    before?: string;
    dagId: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseAssetServiceGetDagAssetQueuedEventsKeyFn(
      { before, dagId },
      queryKey,
    ),
    queryFn: () =>
      AssetService.getDagAssetQueuedEvents({ before, dagId }) as TData,
    ...options,
  });
/**
 * Get Dag Asset Queued Event
 * Get a queued asset event for a DAG.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.uri
 * @param data.before
 * @returns QueuedEventResponse Successful Response
 * @throws ApiError
 */
export const useAssetServiceGetDagAssetQueuedEvent = <
  TData = Common.AssetServiceGetDagAssetQueuedEventDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    before,
    dagId,
    uri,
  }: {
    before?: string;
    dagId: string;
    uri: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseAssetServiceGetDagAssetQueuedEventKeyFn(
      { before, dagId, uri },
      queryKey,
    ),
    queryFn: () =>
      AssetService.getDagAssetQueuedEvent({ before, dagId, uri }) as TData,
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
    endDate?: string;
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
 * List Backfills
 * @param data The data for the request.
 * @param data.dagId
 * @param data.limit
 * @param data.offset
 * @param data.orderBy
 * @returns BackfillCollectionResponse Successful Response
 * @throws ApiError
 */
export const useBackfillServiceListBackfills = <
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
  useQuery<TData, TError>({
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
 * @returns BackfillResponse Successful Response
 * @throws ApiError
 */
export const useBackfillServiceGetBackfill = <
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
  useQuery<TData, TError>({
    queryKey: Common.UseBackfillServiceGetBackfillKeyFn(
      { backfillId },
      queryKey,
    ),
    queryFn: () => BackfillService.getBackfill({ backfillId }) as TData,
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
 * Get Upstream Asset Events
 * If dag run is asset-triggered, return the asset events that triggered it.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @returns AssetEventCollectionResponse Successful Response
 * @throws ApiError
 */
export const useDagRunServiceGetUpstreamAssetEvents = <
  TData = Common.DagRunServiceGetUpstreamAssetEventsDefaultResponse,
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
    queryKey: Common.UseDagRunServiceGetUpstreamAssetEventsKeyFn(
      { dagId, dagRunId },
      queryKey,
    ),
    queryFn: () =>
      DagRunService.getUpstreamAssetEvents({ dagId, dagRunId }) as TData,
    ...options,
  });
/**
 * Get Dag Source
 * Get source code using file token.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.versionNumber
 * @param data.accept
 * @returns DAGSourceResponse Successful Response
 * @throws ApiError
 */
export const useDagSourceServiceGetDagSource = <
  TData = Common.DagSourceServiceGetDagSourceDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    accept,
    dagId,
    versionNumber,
  }: {
    accept?: string;
    dagId: string;
    versionNumber?: number;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDagSourceServiceGetDagSourceKeyFn(
      { accept, dagId, versionNumber },
      queryKey,
    ),
    queryFn: () =>
      DagSourceService.getDagSource({ accept, dagId, versionNumber }) as TData,
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
export const useDagStatsServiceGetDagStats = <
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
  useQuery<TData, TError>({
    queryKey: Common.UseDagStatsServiceGetDagStatsKeyFn({ dagIds }, queryKey),
    queryFn: () => DagStatsService.getDagStats({ dagIds }) as TData,
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
export const useDagWarningServiceListDagWarnings = <
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
  useQuery<TData, TError>({
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
export const useEventLogServiceGetEventLogs = <
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
  useQuery<TData, TError>({
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
export const useImportErrorServiceGetImportError = <
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
  useQuery<TData, TError>({
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
export const useImportErrorServiceGetImportErrors = <
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
  useQuery<TData, TError>({
    queryKey: Common.UseImportErrorServiceGetImportErrorsKeyFn(
      { limit, offset, orderBy },
      queryKey,
    ),
    queryFn: () =>
      ImportErrorService.getImportErrors({ limit, offset, orderBy }) as TData,
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
 * Get Task Instance
 * Get task instance.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @param data.taskId
 * @returns TaskInstanceResponse Successful Response
 * @throws ApiError
 */
export const useTaskInstanceServiceGetTaskInstance = <
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
  useQuery<TData, TError>({
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
export const useTaskInstanceServiceGetMappedTaskInstances = <
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
  useQuery<TData, TError>({
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
export const useTaskInstanceServiceGetTaskInstanceDependencies = <
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
  useQuery<TData, TError>({
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
export const useTaskInstanceServiceGetTaskInstanceDependencies1 = <
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
  useQuery<TData, TError>({
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
export const useTaskInstanceServiceGetMappedTaskInstance = <
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
  useQuery<TData, TError>({
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
export const useTaskInstanceServiceGetTaskInstances = <
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
  useQuery<TData, TError>({
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
 * Get Task Instance Try Details
 * Get task instance details by try number.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @param data.taskId
 * @param data.taskTryNumber
 * @param data.mapIndex
 * @returns TaskInstanceHistoryResponse Successful Response
 * @throws ApiError
 */
export const useTaskInstanceServiceGetTaskInstanceTryDetails = <
  TData = Common.TaskInstanceServiceGetTaskInstanceTryDetailsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    dagRunId,
    mapIndex,
    taskId,
    taskTryNumber,
  }: {
    dagId: string;
    dagRunId: string;
    mapIndex?: number;
    taskId: string;
    taskTryNumber: number;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseTaskInstanceServiceGetTaskInstanceTryDetailsKeyFn(
      { dagId, dagRunId, mapIndex, taskId, taskTryNumber },
      queryKey,
    ),
    queryFn: () =>
      TaskInstanceService.getTaskInstanceTryDetails({
        dagId,
        dagRunId,
        mapIndex,
        taskId,
        taskTryNumber,
      }) as TData,
    ...options,
  });
/**
 * Get Mapped Task Instance Try Details
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @param data.taskId
 * @param data.taskTryNumber
 * @param data.mapIndex
 * @returns TaskInstanceHistoryResponse Successful Response
 * @throws ApiError
 */
export const useTaskInstanceServiceGetMappedTaskInstanceTryDetails = <
  TData = Common.TaskInstanceServiceGetMappedTaskInstanceTryDetailsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    dagRunId,
    mapIndex,
    taskId,
    taskTryNumber,
  }: {
    dagId: string;
    dagRunId: string;
    mapIndex: number;
    taskId: string;
    taskTryNumber: number;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstanceTryDetailsKeyFn(
      { dagId, dagRunId, mapIndex, taskId, taskTryNumber },
      queryKey,
    ),
    queryFn: () =>
      TaskInstanceService.getMappedTaskInstanceTryDetails({
        dagId,
        dagRunId,
        mapIndex,
        taskId,
        taskTryNumber,
      }) as TData,
    ...options,
  });
/**
 * Get Tasks
 * Get tasks for DAG.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.orderBy
 * @returns TaskCollectionResponse Successful Response
 * @throws ApiError
 */
export const useTaskServiceGetTasks = <
  TData = Common.TaskServiceGetTasksDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    orderBy,
  }: {
    dagId: string;
    orderBy?: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseTaskServiceGetTasksKeyFn({ dagId, orderBy }, queryKey),
    queryFn: () => TaskService.getTasks({ dagId, orderBy }) as TData,
    ...options,
  });
/**
 * Get Task
 * Get simplified representation of a task.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.taskId
 * @returns TaskResponse Successful Response
 * @throws ApiError
 */
export const useTaskServiceGetTask = <
  TData = Common.TaskServiceGetTaskDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    taskId,
  }: {
    dagId: string;
    taskId: unknown;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseTaskServiceGetTaskKeyFn({ dagId, taskId }, queryKey),
    queryFn: () => TaskService.getTask({ dagId, taskId }) as TData,
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
export const useXcomServiceGetXcomEntry = <
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
  useQuery<TData, TError>({
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
 * Create Asset Event
 * Create asset events.
 * @param data The data for the request.
 * @param data.requestBody
 * @returns AssetEventResponse Successful Response
 * @throws ApiError
 */
export const useAssetServiceCreateAssetEvent = <
  TData = Common.AssetServiceCreateAssetEventMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        requestBody: CreateAssetEventsBody;
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
      requestBody: CreateAssetEventsBody;
    },
    TContext
  >({
    mutationFn: ({ requestBody }) =>
      AssetService.createAssetEvent({
        requestBody,
      }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Create Backfill
 * @param data The data for the request.
 * @param data.requestBody
 * @returns BackfillResponse Successful Response
 * @throws ApiError
 */
export const useBackfillServiceCreateBackfill = <
  TData = Common.BackfillServiceCreateBackfillMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        requestBody: BackfillPostBody;
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
      requestBody: BackfillPostBody;
    },
    TContext
  >({
    mutationFn: ({ requestBody }) =>
      BackfillService.createBackfill({
        requestBody,
      }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Post Connection
 * Create connection entry.
 * @param data The data for the request.
 * @param data.requestBody
 * @returns ConnectionResponse Successful Response
 * @throws ApiError
 */
export const useConnectionServicePostConnection = <
  TData = Common.ConnectionServicePostConnectionMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        requestBody: ConnectionBody;
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
      requestBody: ConnectionBody;
    },
    TContext
  >({
    mutationFn: ({ requestBody }) =>
      ConnectionService.postConnection({
        requestBody,
      }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Test Connection
 * Test an API connection.
 *
 * This method first creates an in-memory transient conn_id & exports that to an env var,
 * as some hook classes tries to find out the `conn` from their __init__ method & errors out if not found.
 * It also deletes the conn id env variable after the test.
 * @param data The data for the request.
 * @param data.requestBody
 * @returns ConnectionTestResponse Successful Response
 * @throws ApiError
 */
export const useConnectionServiceTestConnection = <
  TData = Common.ConnectionServiceTestConnectionMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        requestBody: ConnectionBody;
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
      requestBody: ConnectionBody;
    },
    TContext
  >({
    mutationFn: ({ requestBody }) =>
      ConnectionService.testConnection({
        requestBody,
      }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Clear Dag Run
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @param data.requestBody
 * @returns unknown Successful Response
 * @throws ApiError
 */
export const useDagRunServiceClearDagRun = <
  TData = Common.DagRunServiceClearDagRunMutationResult,
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
        requestBody: DAGRunClearBody;
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
      requestBody: DAGRunClearBody;
    },
    TContext
  >({
    mutationFn: ({ dagId, dagRunId, requestBody }) =>
      DagRunService.clearDagRun({
        dagId,
        dagRunId,
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
 * Get Task Instances Batch
 * Get list of task instances.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @param data.requestBody
 * @returns TaskInstanceCollectionResponse Successful Response
 * @throws ApiError
 */
export const useTaskInstanceServiceGetTaskInstancesBatch = <
  TData = Common.TaskInstanceServiceGetTaskInstancesBatchMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        dagId: "~";
        dagRunId: "~";
        requestBody: TaskInstancesBatchBody;
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
      dagId: "~";
      dagRunId: "~";
      requestBody: TaskInstancesBatchBody;
    },
    TContext
  >({
    mutationFn: ({ dagId, dagRunId, requestBody }) =>
      TaskInstanceService.getTaskInstancesBatch({
        dagId,
        dagRunId,
        requestBody,
      }) as unknown as Promise<TData>,
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
 * Pause Backfill
 * @param data The data for the request.
 * @param data.backfillId
 * @returns BackfillResponse Successful Response
 * @throws ApiError
 */
export const useBackfillServicePauseBackfill = <
  TData = Common.BackfillServicePauseBackfillMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        backfillId: unknown;
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
      backfillId: unknown;
    },
    TContext
  >({
    mutationFn: ({ backfillId }) =>
      BackfillService.pauseBackfill({
        backfillId,
      }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Unpause Backfill
 * @param data The data for the request.
 * @param data.backfillId
 * @returns BackfillResponse Successful Response
 * @throws ApiError
 */
export const useBackfillServiceUnpauseBackfill = <
  TData = Common.BackfillServiceUnpauseBackfillMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        backfillId: unknown;
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
      backfillId: unknown;
    },
    TContext
  >({
    mutationFn: ({ backfillId }) =>
      BackfillService.unpauseBackfill({
        backfillId,
      }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Cancel Backfill
 * @param data The data for the request.
 * @param data.backfillId
 * @returns BackfillResponse Successful Response
 * @throws ApiError
 */
export const useBackfillServiceCancelBackfill = <
  TData = Common.BackfillServiceCancelBackfillMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        backfillId: unknown;
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
      backfillId: unknown;
    },
    TContext
  >({
    mutationFn: ({ backfillId }) =>
      BackfillService.cancelBackfill({
        backfillId,
      }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Patch Connection
 * Update a connection entry.
 * @param data The data for the request.
 * @param data.connectionId
 * @param data.requestBody
 * @param data.updateMask
 * @returns ConnectionResponse Successful Response
 * @throws ApiError
 */
export const useConnectionServicePatchConnection = <
  TData = Common.ConnectionServicePatchConnectionMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        connectionId: string;
        requestBody: ConnectionBody;
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
      connectionId: string;
      requestBody: ConnectionBody;
      updateMask?: string[];
    },
    TContext
  >({
    mutationFn: ({ connectionId, requestBody, updateMask }) =>
      ConnectionService.patchConnection({
        connectionId,
        requestBody,
        updateMask,
      }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Patch Dag Run
 * Modify a DAG Run.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @param data.requestBody
 * @param data.updateMask
 * @returns DAGRunResponse Successful Response
 * @throws ApiError
 */
export const useDagRunServicePatchDagRun = <
  TData = Common.DagRunServicePatchDagRunMutationResult,
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
        requestBody: DAGRunPatchBody;
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
      dagRunId: string;
      requestBody: DAGRunPatchBody;
      updateMask?: string[];
    },
    TContext
  >({
    mutationFn: ({ dagId, dagRunId, requestBody, updateMask }) =>
      DagRunService.patchDagRun({
        dagId,
        dagRunId,
        requestBody,
        updateMask,
      }) as unknown as Promise<TData>,
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
 * Delete Asset Queued Events
 * Delete queued asset events for an asset.
 * @param data The data for the request.
 * @param data.uri
 * @param data.before
 * @returns void Successful Response
 * @throws ApiError
 */
export const useAssetServiceDeleteAssetQueuedEvents = <
  TData = Common.AssetServiceDeleteAssetQueuedEventsMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        before?: string;
        uri: string;
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
      before?: string;
      uri: string;
    },
    TContext
  >({
    mutationFn: ({ before, uri }) =>
      AssetService.deleteAssetQueuedEvents({
        before,
        uri,
      }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Delete Dag Asset Queued Events
 * @param data The data for the request.
 * @param data.dagId
 * @param data.before
 * @returns void Successful Response
 * @throws ApiError
 */
export const useAssetServiceDeleteDagAssetQueuedEvents = <
  TData = Common.AssetServiceDeleteDagAssetQueuedEventsMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        before?: string;
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
      before?: string;
      dagId: string;
    },
    TContext
  >({
    mutationFn: ({ before, dagId }) =>
      AssetService.deleteDagAssetQueuedEvents({
        before,
        dagId,
      }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Delete Dag Asset Queued Event
 * Delete a queued asset event for a DAG.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.uri
 * @param data.before
 * @returns void Successful Response
 * @throws ApiError
 */
export const useAssetServiceDeleteDagAssetQueuedEvent = <
  TData = Common.AssetServiceDeleteDagAssetQueuedEventMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        before?: string;
        dagId: string;
        uri: string;
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
      before?: string;
      dagId: string;
      uri: string;
    },
    TContext
  >({
    mutationFn: ({ before, dagId, uri }) =>
      AssetService.deleteDagAssetQueuedEvent({
        before,
        dagId,
        uri,
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
