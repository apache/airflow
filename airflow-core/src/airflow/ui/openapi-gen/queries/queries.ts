// generated with @7nohe/openapi-react-query-codegen@1.6.2
import { UseMutationOptions, UseQueryOptions, useMutation, useQuery } from "@tanstack/react-query";

import {
  AssetService,
  AuthLinksService,
  BackfillService,
  ConfigService,
  ConnectionService,
  DagParsingService,
  DagReportService,
  DagRunService,
  DagService,
  DagSourceService,
  DagStatsService,
  DagVersionService,
  DagWarningService,
  DashboardService,
  DependenciesService,
  EventLogService,
  ExtraLinksService,
  GridService,
  ImportErrorService,
  JobService,
  LoginService,
  MonitorService,
  PluginService,
  PoolService,
  ProviderService,
  StructureService,
  TaskInstanceService,
  TaskService,
  VariableService,
  VersionService,
  XcomService,
} from "../requests/services.gen";
import {
  BackfillPostBody,
  BulkBody_ConnectionBody_,
  BulkBody_PoolBody_,
  BulkBody_VariableBody_,
  ClearTaskInstancesBody,
  ConnectionBody,
  CreateAssetEventsBody,
  DAGPatchBody,
  DAGRunClearBody,
  DAGRunPatchBody,
  DAGRunsBatchBody,
  DagRunState,
  DagWarningType,
  PatchTaskInstanceBody,
  PoolBody,
  PoolPatchBody,
  TaskInstancesBatchBody,
  TriggerDAGRunPostBody,
  VariableBody,
  XComCreateBody,
  XComUpdateBody,
} from "../requests/types.gen";
import * as Common from "./common";

/**
 * Get Assets
 * Get assets.
 * @param data The data for the request.
 * @param data.limit
 * @param data.offset
 * @param data.namePattern
 * @param data.uriPattern
 * @param data.dagIds
 * @param data.onlyActive
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
    namePattern,
    offset,
    onlyActive,
    orderBy,
    uriPattern,
  }: {
    dagIds?: string[];
    limit?: number;
    namePattern?: string;
    offset?: number;
    onlyActive?: boolean;
    orderBy?: string;
    uriPattern?: string;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseAssetServiceGetAssetsKeyFn(
      { dagIds, limit, namePattern, offset, onlyActive, orderBy, uriPattern },
      queryKey,
    ),
    queryFn: () =>
      AssetService.getAssets({
        dagIds,
        limit,
        namePattern,
        offset,
        onlyActive,
        orderBy,
        uriPattern,
      }) as TData,
    ...options,
  });
/**
 * Get Asset Aliases
 * Get asset aliases.
 * @param data The data for the request.
 * @param data.limit
 * @param data.offset
 * @param data.namePattern
 * @param data.orderBy
 * @returns AssetAliasCollectionResponse Successful Response
 * @throws ApiError
 */
export const useAssetServiceGetAssetAliases = <
  TData = Common.AssetServiceGetAssetAliasesDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    limit,
    namePattern,
    offset,
    orderBy,
  }: {
    limit?: number;
    namePattern?: string;
    offset?: number;
    orderBy?: string;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseAssetServiceGetAssetAliasesKeyFn({ limit, namePattern, offset, orderBy }, queryKey),
    queryFn: () => AssetService.getAssetAliases({ limit, namePattern, offset, orderBy }) as TData,
    ...options,
  });
/**
 * Get Asset Alias
 * Get an asset alias.
 * @param data The data for the request.
 * @param data.assetAliasId
 * @returns unknown Successful Response
 * @throws ApiError
 */
export const useAssetServiceGetAssetAlias = <
  TData = Common.AssetServiceGetAssetAliasDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    assetAliasId,
  }: {
    assetAliasId: number;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseAssetServiceGetAssetAliasKeyFn({ assetAliasId }, queryKey),
    queryFn: () => AssetService.getAssetAlias({ assetAliasId }) as TData,
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
 * @param data.timestampGte
 * @param data.timestampLte
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
    timestampGte,
    timestampLte,
  }: {
    assetId?: number;
    limit?: number;
    offset?: number;
    orderBy?: string;
    sourceDagId?: string;
    sourceMapIndex?: number;
    sourceRunId?: string;
    sourceTaskId?: string;
    timestampGte?: string;
    timestampLte?: string;
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
        timestampGte,
        timestampLte,
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
        timestampGte,
        timestampLte,
      }) as TData,
    ...options,
  });
/**
 * Get Asset Queued Events
 * Get queued asset events for an asset.
 * @param data The data for the request.
 * @param data.assetId
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
    assetId,
    before,
  }: {
    assetId: number;
    before?: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseAssetServiceGetAssetQueuedEventsKeyFn({ assetId, before }, queryKey),
    queryFn: () => AssetService.getAssetQueuedEvents({ assetId, before }) as TData,
    ...options,
  });
/**
 * Get Asset
 * Get an asset.
 * @param data The data for the request.
 * @param data.assetId
 * @returns AssetResponse Successful Response
 * @throws ApiError
 */
export const useAssetServiceGetAsset = <
  TData = Common.AssetServiceGetAssetDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    assetId,
  }: {
    assetId: number;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseAssetServiceGetAssetKeyFn({ assetId }, queryKey),
    queryFn: () => AssetService.getAsset({ assetId }) as TData,
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
    queryKey: Common.UseAssetServiceGetDagAssetQueuedEventsKeyFn({ before, dagId }, queryKey),
    queryFn: () => AssetService.getDagAssetQueuedEvents({ before, dagId }) as TData,
    ...options,
  });
/**
 * Get Dag Asset Queued Event
 * Get a queued asset event for a DAG.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.assetId
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
    assetId,
    before,
    dagId,
  }: {
    assetId: number;
    before?: string;
    dagId: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseAssetServiceGetDagAssetQueuedEventKeyFn({ assetId, before, dagId }, queryKey),
    queryFn: () => AssetService.getDagAssetQueuedEvent({ assetId, before, dagId }) as TData,
    ...options,
  });
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
    queryKey: Common.UseBackfillServiceListBackfillsKeyFn({ dagId, limit, offset, orderBy }, queryKey),
    queryFn: () => BackfillService.listBackfills({ dagId, limit, offset, orderBy }) as TData,
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
    backfillId: number;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseBackfillServiceGetBackfillKeyFn({ backfillId }, queryKey),
    queryFn: () => BackfillService.getBackfill({ backfillId }) as TData,
    ...options,
  });
/**
 * List Backfills
 * @param data The data for the request.
 * @param data.limit
 * @param data.offset
 * @param data.orderBy
 * @param data.dagId
 * @param data.active
 * @returns BackfillCollectionResponse Successful Response
 * @throws ApiError
 */
export const useBackfillServiceListBackfills1 = <
  TData = Common.BackfillServiceListBackfills1DefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    active,
    dagId,
    limit,
    offset,
    orderBy,
  }: {
    active?: boolean;
    dagId?: string;
    limit?: number;
    offset?: number;
    orderBy?: string;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseBackfillServiceListBackfills1KeyFn(
      { active, dagId, limit, offset, orderBy },
      queryKey,
    ),
    queryFn: () => BackfillService.listBackfills1({ active, dagId, limit, offset, orderBy }) as TData,
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
    queryKey: Common.UseConnectionServiceGetConnectionKeyFn({ connectionId }, queryKey),
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
 * @param data.connectionIdPattern
 * @returns ConnectionCollectionResponse Successful Response
 * @throws ApiError
 */
export const useConnectionServiceGetConnections = <
  TData = Common.ConnectionServiceGetConnectionsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    connectionIdPattern,
    limit,
    offset,
    orderBy,
  }: {
    connectionIdPattern?: string;
    limit?: number;
    offset?: number;
    orderBy?: string;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseConnectionServiceGetConnectionsKeyFn(
      { connectionIdPattern, limit, offset, orderBy },
      queryKey,
    ),
    queryFn: () => ConnectionService.getConnections({ connectionIdPattern, limit, offset, orderBy }) as TData,
    ...options,
  });
/**
 * Hook Meta Data
 * Retrieve information about available connection types (hook classes) and their parameters.
 * @returns ConnectionHookMetaData Successful Response
 * @throws ApiError
 */
export const useConnectionServiceHookMetaData = <
  TData = Common.ConnectionServiceHookMetaDataDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseConnectionServiceHookMetaDataKeyFn(queryKey),
    queryFn: () => ConnectionService.hookMetaData() as TData,
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
    queryKey: Common.UseDagRunServiceGetDagRunKeyFn({ dagId, dagRunId }, queryKey),
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
    queryKey: Common.UseDagRunServiceGetUpstreamAssetEventsKeyFn({ dagId, dagRunId }, queryKey),
    queryFn: () => DagRunService.getUpstreamAssetEvents({ dagId, dagRunId }) as TData,
    ...options,
  });
/**
 * Get Dag Runs
 * Get all DAG Runs.
 *
 * This endpoint allows specifying `~` as the dag_id to retrieve Dag Runs for all DAGs.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.limit
 * @param data.offset
 * @param data.runAfterGte
 * @param data.runAfterLte
 * @param data.logicalDateGte
 * @param data.logicalDateLte
 * @param data.startDateGte
 * @param data.startDateLte
 * @param data.endDateGte
 * @param data.endDateLte
 * @param data.updatedAtGte
 * @param data.updatedAtLte
 * @param data.runType
 * @param data.state
 * @param data.orderBy
 * @returns DAGRunCollectionResponse Successful Response
 * @throws ApiError
 */
export const useDagRunServiceGetDagRuns = <
  TData = Common.DagRunServiceGetDagRunsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    endDateGte,
    endDateLte,
    limit,
    logicalDateGte,
    logicalDateLte,
    offset,
    orderBy,
    runAfterGte,
    runAfterLte,
    runType,
    startDateGte,
    startDateLte,
    state,
    updatedAtGte,
    updatedAtLte,
  }: {
    dagId: string;
    endDateGte?: string;
    endDateLte?: string;
    limit?: number;
    logicalDateGte?: string;
    logicalDateLte?: string;
    offset?: number;
    orderBy?: string;
    runAfterGte?: string;
    runAfterLte?: string;
    runType?: string[];
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
    queryKey: Common.UseDagRunServiceGetDagRunsKeyFn(
      {
        dagId,
        endDateGte,
        endDateLte,
        limit,
        logicalDateGte,
        logicalDateLte,
        offset,
        orderBy,
        runAfterGte,
        runAfterLte,
        runType,
        startDateGte,
        startDateLte,
        state,
        updatedAtGte,
        updatedAtLte,
      },
      queryKey,
    ),
    queryFn: () =>
      DagRunService.getDagRuns({
        dagId,
        endDateGte,
        endDateLte,
        limit,
        logicalDateGte,
        logicalDateLte,
        offset,
        orderBy,
        runAfterGte,
        runAfterLte,
        runType,
        startDateGte,
        startDateLte,
        state,
        updatedAtGte,
        updatedAtLte,
      }) as TData,
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
    accept?: "application/json" | "text/plain" | "*/*";
    dagId: string;
    versionNumber?: number;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDagSourceServiceGetDagSourceKeyFn({ accept, dagId, versionNumber }, queryKey),
    queryFn: () => DagSourceService.getDagSource({ accept, dagId, versionNumber }) as TData,
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
 * Get Dag Reports
 * Get DAG report.
 * @param data The data for the request.
 * @param data.subdir
 * @returns unknown Successful Response
 * @throws ApiError
 */
export const useDagReportServiceGetDagReports = <
  TData = Common.DagReportServiceGetDagReportsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    subdir,
  }: {
    subdir: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDagReportServiceGetDagReportsKeyFn({ subdir }, queryKey),
    queryFn: () => DagReportService.getDagReports({ subdir }) as TData,
    ...options,
  });
/**
 * Get Config
 * @param data The data for the request.
 * @param data.section
 * @param data.accept
 * @returns Config Successful Response
 * @throws ApiError
 */
export const useConfigServiceGetConfig = <
  TData = Common.ConfigServiceGetConfigDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    accept,
    section,
  }: {
    accept?: "application/json" | "text/plain" | "*/*";
    section?: string;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseConfigServiceGetConfigKeyFn({ accept, section }, queryKey),
    queryFn: () => ConfigService.getConfig({ accept, section }) as TData,
    ...options,
  });
/**
 * Get Config Value
 * @param data The data for the request.
 * @param data.section
 * @param data.option
 * @param data.accept
 * @returns Config Successful Response
 * @throws ApiError
 */
export const useConfigServiceGetConfigValue = <
  TData = Common.ConfigServiceGetConfigValueDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    accept,
    option,
    section,
  }: {
    accept?: "application/json" | "text/plain" | "*/*";
    option: string;
    section: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseConfigServiceGetConfigValueKeyFn({ accept, option, section }, queryKey),
    queryFn: () => ConfigService.getConfigValue({ accept, option, section }) as TData,
    ...options,
  });
/**
 * Get Configs
 * Get configs for UI.
 * @returns ConfigResponse Successful Response
 * @throws ApiError
 */
export const useConfigServiceGetConfigs = <
  TData = Common.ConfigServiceGetConfigsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseConfigServiceGetConfigsKeyFn(queryKey),
    queryFn: () => ConfigService.getConfigs() as TData,
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
    queryFn: () => DagWarningService.listDagWarnings({ dagId, limit, offset, orderBy, warningType }) as TData,
    ...options,
  });
/**
 * Get Dags
 * Get all DAGs.
 * @param data The data for the request.
 * @param data.limit
 * @param data.offset
 * @param data.tags
 * @param data.tagsMatchMode
 * @param data.owners
 * @param data.dagIdPattern
 * @param data.dagDisplayNamePattern
 * @param data.excludeStale
 * @param data.paused
 * @param data.lastDagRunState
 * @param data.dagRunStartDateGte
 * @param data.dagRunStartDateLte
 * @param data.dagRunEndDateGte
 * @param data.dagRunEndDateLte
 * @param data.dagRunState
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
    dagRunEndDateGte,
    dagRunEndDateLte,
    dagRunStartDateGte,
    dagRunStartDateLte,
    dagRunState,
    excludeStale,
    lastDagRunState,
    limit,
    offset,
    orderBy,
    owners,
    paused,
    tags,
    tagsMatchMode,
  }: {
    dagDisplayNamePattern?: string;
    dagIdPattern?: string;
    dagRunEndDateGte?: string;
    dagRunEndDateLte?: string;
    dagRunStartDateGte?: string;
    dagRunStartDateLte?: string;
    dagRunState?: string[];
    excludeStale?: boolean;
    lastDagRunState?: DagRunState;
    limit?: number;
    offset?: number;
    orderBy?: string;
    owners?: string[];
    paused?: boolean;
    tags?: string[];
    tagsMatchMode?: "any" | "all";
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDagServiceGetDagsKeyFn(
      {
        dagDisplayNamePattern,
        dagIdPattern,
        dagRunEndDateGte,
        dagRunEndDateLte,
        dagRunStartDateGte,
        dagRunStartDateLte,
        dagRunState,
        excludeStale,
        lastDagRunState,
        limit,
        offset,
        orderBy,
        owners,
        paused,
        tags,
        tagsMatchMode,
      },
      queryKey,
    ),
    queryFn: () =>
      DagService.getDags({
        dagDisplayNamePattern,
        dagIdPattern,
        dagRunEndDateGte,
        dagRunEndDateLte,
        dagRunStartDateGte,
        dagRunStartDateLte,
        dagRunState,
        excludeStale,
        lastDagRunState,
        limit,
        offset,
        orderBy,
        owners,
        paused,
        tags,
        tagsMatchMode,
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
    queryKey: Common.UseDagServiceGetDagTagsKeyFn({ limit, offset, orderBy, tagNamePattern }, queryKey),
    queryFn: () => DagService.getDagTags({ limit, offset, orderBy, tagNamePattern }) as TData,
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
 * @param data.tagsMatchMode
 * @param data.owners
 * @param data.dagIds
 * @param data.dagIdPattern
 * @param data.dagDisplayNamePattern
 * @param data.excludeStale
 * @param data.paused
 * @param data.lastDagRunState
 * @returns DAGWithLatestDagRunsCollectionResponse Successful Response
 * @throws ApiError
 */
export const useDagServiceRecentDagRuns = <
  TData = Common.DagServiceRecentDagRunsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagDisplayNamePattern,
    dagIdPattern,
    dagIds,
    dagRunsLimit,
    excludeStale,
    lastDagRunState,
    limit,
    offset,
    owners,
    paused,
    tags,
    tagsMatchMode,
  }: {
    dagDisplayNamePattern?: string;
    dagIdPattern?: string;
    dagIds?: string[];
    dagRunsLimit?: number;
    excludeStale?: boolean;
    lastDagRunState?: DagRunState;
    limit?: number;
    offset?: number;
    owners?: string[];
    paused?: boolean;
    tags?: string[];
    tagsMatchMode?: "any" | "all";
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDagServiceRecentDagRunsKeyFn(
      {
        dagDisplayNamePattern,
        dagIdPattern,
        dagIds,
        dagRunsLimit,
        excludeStale,
        lastDagRunState,
        limit,
        offset,
        owners,
        paused,
        tags,
        tagsMatchMode,
      },
      queryKey,
    ),
    queryFn: () =>
      DagService.recentDagRuns({
        dagDisplayNamePattern,
        dagIdPattern,
        dagIds,
        dagRunsLimit,
        excludeStale,
        lastDagRunState,
        limit,
        offset,
        owners,
        paused,
        tags,
        tagsMatchMode,
      }) as TData,
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
    queryKey: Common.UseEventLogServiceGetEventLogKeyFn({ eventLogId }, queryKey),
    queryFn: () => EventLogService.getEventLog({ eventLogId }) as TData,
    ...options,
  });
/**
 * Get Event Logs
 * Get all Event Logs.
 * @param data The data for the request.
 * @param data.limit
 * @param data.offset
 * @param data.orderBy
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
 * Get Extra Links
 * Get extra links for task instance.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @param data.taskId
 * @param data.mapIndex
 * @returns ExtraLinkCollectionResponse Successful Response
 * @throws ApiError
 */
export const useExtraLinksServiceGetExtraLinks = <
  TData = Common.ExtraLinksServiceGetExtraLinksDefaultResponse,
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
    queryKey: Common.UseExtraLinksServiceGetExtraLinksKeyFn({ dagId, dagRunId, mapIndex, taskId }, queryKey),
    queryFn: () => ExtraLinksService.getExtraLinks({ dagId, dagRunId, mapIndex, taskId }) as TData,
    ...options,
  });
/**
 * Get Extra Links
 * Get extra links for task instance.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @param data.taskId
 * @param data.mapIndex
 * @returns ExtraLinkCollectionResponse Successful Response
 * @throws ApiError
 */
export const useTaskInstanceServiceGetExtraLinks = <
  TData = Common.TaskInstanceServiceGetExtraLinksDefaultResponse,
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
    queryKey: Common.UseTaskInstanceServiceGetExtraLinksKeyFn(
      { dagId, dagRunId, mapIndex, taskId },
      queryKey,
    ),
    queryFn: () => TaskInstanceService.getExtraLinks({ dagId, dagRunId, mapIndex, taskId }) as TData,
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
    queryKey: Common.UseTaskInstanceServiceGetTaskInstanceKeyFn({ dagId, dagRunId, taskId }, queryKey),
    queryFn: () => TaskInstanceService.getTaskInstance({ dagId, dagRunId, taskId }) as TData,
    ...options,
  });
/**
 * Get Mapped Task Instances
 * Get list of mapped task instances.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @param data.taskId
 * @param data.runAfterGte
 * @param data.runAfterLte
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
 * @param data.versionNumber
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
    runAfterGte,
    runAfterLte,
    startDateGte,
    startDateLte,
    state,
    taskId,
    updatedAtGte,
    updatedAtLte,
    versionNumber,
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
    runAfterGte?: string;
    runAfterLte?: string;
    startDateGte?: string;
    startDateLte?: string;
    state?: string[];
    taskId: string;
    updatedAtGte?: string;
    updatedAtLte?: string;
    versionNumber?: number[];
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
        runAfterGte,
        runAfterLte,
        startDateGte,
        startDateLte,
        state,
        taskId,
        updatedAtGte,
        updatedAtLte,
        versionNumber,
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
        runAfterGte,
        runAfterLte,
        startDateGte,
        startDateLte,
        state,
        taskId,
        updatedAtGte,
        updatedAtLte,
        versionNumber,
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
export const useTaskInstanceServiceGetTaskInstanceDependenciesByMapIndex = <
  TData = Common.TaskInstanceServiceGetTaskInstanceDependenciesByMapIndexDefaultResponse,
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
    queryKey: Common.UseTaskInstanceServiceGetTaskInstanceDependenciesByMapIndexKeyFn(
      { dagId, dagRunId, mapIndex, taskId },
      queryKey,
    ),
    queryFn: () =>
      TaskInstanceService.getTaskInstanceDependenciesByMapIndex({
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
    mapIndex?: number;
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
      TaskInstanceService.getTaskInstanceDependencies({ dagId, dagRunId, mapIndex, taskId }) as TData,
    ...options,
  });
/**
 * Get Task Instance Tries
 * Get list of task instances history.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @param data.taskId
 * @param data.mapIndex
 * @returns TaskInstanceHistoryCollectionResponse Successful Response
 * @throws ApiError
 */
export const useTaskInstanceServiceGetTaskInstanceTries = <
  TData = Common.TaskInstanceServiceGetTaskInstanceTriesDefaultResponse,
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
    queryKey: Common.UseTaskInstanceServiceGetTaskInstanceTriesKeyFn(
      { dagId, dagRunId, mapIndex, taskId },
      queryKey,
    ),
    queryFn: () => TaskInstanceService.getTaskInstanceTries({ dagId, dagRunId, mapIndex, taskId }) as TData,
    ...options,
  });
/**
 * Get Mapped Task Instance Tries
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @param data.taskId
 * @param data.mapIndex
 * @returns TaskInstanceHistoryCollectionResponse Successful Response
 * @throws ApiError
 */
export const useTaskInstanceServiceGetMappedTaskInstanceTries = <
  TData = Common.TaskInstanceServiceGetMappedTaskInstanceTriesDefaultResponse,
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
    queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstanceTriesKeyFn(
      { dagId, dagRunId, mapIndex, taskId },
      queryKey,
    ),
    queryFn: () =>
      TaskInstanceService.getMappedTaskInstanceTries({ dagId, dagRunId, mapIndex, taskId }) as TData,
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
    queryFn: () => TaskInstanceService.getMappedTaskInstance({ dagId, dagRunId, mapIndex, taskId }) as TData,
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
 * @param data.taskId
 * @param data.runAfterGte
 * @param data.runAfterLte
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
 * @param data.taskDisplayNamePattern
 * @param data.state
 * @param data.pool
 * @param data.queue
 * @param data.executor
 * @param data.versionNumber
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
    runAfterGte,
    runAfterLte,
    startDateGte,
    startDateLte,
    state,
    taskDisplayNamePattern,
    taskId,
    updatedAtGte,
    updatedAtLte,
    versionNumber,
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
    runAfterGte?: string;
    runAfterLte?: string;
    startDateGte?: string;
    startDateLte?: string;
    state?: string[];
    taskDisplayNamePattern?: string;
    taskId?: string;
    updatedAtGte?: string;
    updatedAtLte?: string;
    versionNumber?: number[];
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
        runAfterGte,
        runAfterLte,
        startDateGte,
        startDateLte,
        state,
        taskDisplayNamePattern,
        taskId,
        updatedAtGte,
        updatedAtLte,
        versionNumber,
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
        runAfterGte,
        runAfterLte,
        startDateGte,
        startDateLte,
        state,
        taskDisplayNamePattern,
        taskId,
        updatedAtGte,
        updatedAtLte,
        versionNumber,
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
 * Get Log
 * Get logs for a specific task instance.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @param data.taskId
 * @param data.tryNumber
 * @param data.fullContent
 * @param data.mapIndex
 * @param data.token
 * @param data.accept
 * @returns TaskInstancesLogResponse Successful Response
 * @throws ApiError
 */
export const useTaskInstanceServiceGetLog = <
  TData = Common.TaskInstanceServiceGetLogDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    accept,
    dagId,
    dagRunId,
    fullContent,
    mapIndex,
    taskId,
    token,
    tryNumber,
  }: {
    accept?: "application/json" | "*/*" | "application/x-ndjson";
    dagId: string;
    dagRunId: string;
    fullContent?: boolean;
    mapIndex?: number;
    taskId: string;
    token?: string;
    tryNumber: number;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseTaskInstanceServiceGetLogKeyFn(
      { accept, dagId, dagRunId, fullContent, mapIndex, taskId, token, tryNumber },
      queryKey,
    ),
    queryFn: () =>
      TaskInstanceService.getLog({
        accept,
        dagId,
        dagRunId,
        fullContent,
        mapIndex,
        taskId,
        token,
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
    queryKey: Common.UseImportErrorServiceGetImportErrorKeyFn({ importErrorId }, queryKey),
    queryFn: () => ImportErrorService.getImportError({ importErrorId }) as TData,
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
    queryKey: Common.UseImportErrorServiceGetImportErrorsKeyFn({ limit, offset, orderBy }, queryKey),
    queryFn: () => ImportErrorService.getImportErrors({ limit, offset, orderBy }) as TData,
    ...options,
  });
/**
 * Get Jobs
 * Get all jobs.
 * @param data The data for the request.
 * @param data.isAlive
 * @param data.startDateGte
 * @param data.startDateLte
 * @param data.endDateGte
 * @param data.endDateLte
 * @param data.limit
 * @param data.offset
 * @param data.orderBy
 * @param data.jobState
 * @param data.jobType
 * @param data.hostname
 * @param data.executorClass
 * @returns JobCollectionResponse Successful Response
 * @throws ApiError
 */
export const useJobServiceGetJobs = <
  TData = Common.JobServiceGetJobsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    endDateGte,
    endDateLte,
    executorClass,
    hostname,
    isAlive,
    jobState,
    jobType,
    limit,
    offset,
    orderBy,
    startDateGte,
    startDateLte,
  }: {
    endDateGte?: string;
    endDateLte?: string;
    executorClass?: string;
    hostname?: string;
    isAlive?: boolean;
    jobState?: string;
    jobType?: string;
    limit?: number;
    offset?: number;
    orderBy?: string;
    startDateGte?: string;
    startDateLte?: string;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseJobServiceGetJobsKeyFn(
      {
        endDateGte,
        endDateLte,
        executorClass,
        hostname,
        isAlive,
        jobState,
        jobType,
        limit,
        offset,
        orderBy,
        startDateGte,
        startDateLte,
      },
      queryKey,
    ),
    queryFn: () =>
      JobService.getJobs({
        endDateGte,
        endDateLte,
        executorClass,
        hostname,
        isAlive,
        jobState,
        jobType,
        limit,
        offset,
        orderBy,
        startDateGte,
        startDateLte,
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
    queryKey: Common.UsePluginServiceGetPluginsKeyFn({ limit, offset }, queryKey),
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
 * @param data.poolNamePattern
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
    poolNamePattern,
  }: {
    limit?: number;
    offset?: number;
    orderBy?: string;
    poolNamePattern?: string;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UsePoolServiceGetPoolsKeyFn({ limit, offset, orderBy, poolNamePattern }, queryKey),
    queryFn: () => PoolService.getPools({ limit, offset, orderBy, poolNamePattern }) as TData,
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
    queryKey: Common.UseProviderServiceGetProvidersKeyFn({ limit, offset }, queryKey),
    queryFn: () => ProviderService.getProviders({ limit, offset }) as TData,
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
 * Get Xcom Entries
 * Get all XCom entries.
 *
 * This endpoint allows specifying `~` as the dag_id, dag_run_id, task_id to retrieve XCom entries for all DAGs.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @param data.taskId
 * @param data.xcomKey
 * @param data.mapIndex
 * @param data.limit
 * @param data.offset
 * @returns XComCollectionResponse Successful Response
 * @throws ApiError
 */
export const useXcomServiceGetXcomEntries = <
  TData = Common.XcomServiceGetXcomEntriesDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    dagRunId,
    limit,
    mapIndex,
    offset,
    taskId,
    xcomKey,
  }: {
    dagId: string;
    dagRunId: string;
    limit?: number;
    mapIndex?: number;
    offset?: number;
    taskId: string;
    xcomKey?: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseXcomServiceGetXcomEntriesKeyFn(
      { dagId, dagRunId, limit, mapIndex, offset, taskId, xcomKey },
      queryKey,
    ),
    queryFn: () =>
      XcomService.getXcomEntries({ dagId, dagRunId, limit, mapIndex, offset, taskId, xcomKey }) as TData,
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
    queryKey: Common.UseVariableServiceGetVariableKeyFn({ variableKey }, queryKey),
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
 * @param data.variableKeyPattern
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
    variableKeyPattern,
  }: {
    limit?: number;
    offset?: number;
    orderBy?: string;
    variableKeyPattern?: string;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseVariableServiceGetVariablesKeyFn(
      { limit, offset, orderBy, variableKeyPattern },
      queryKey,
    ),
    queryFn: () => VariableService.getVariables({ limit, offset, orderBy, variableKeyPattern }) as TData,
    ...options,
  });
/**
 * Get Dag Version
 * Get one Dag Version.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.versionNumber
 * @returns DagVersionResponse Successful Response
 * @throws ApiError
 */
export const useDagVersionServiceGetDagVersion = <
  TData = Common.DagVersionServiceGetDagVersionDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    versionNumber,
  }: {
    dagId: string;
    versionNumber: number;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDagVersionServiceGetDagVersionKeyFn({ dagId, versionNumber }, queryKey),
    queryFn: () => DagVersionService.getDagVersion({ dagId, versionNumber }) as TData,
    ...options,
  });
/**
 * Get Dag Versions
 * Get all DAG Versions.
 *
 * This endpoint allows specifying `~` as the dag_id to retrieve DAG Versions for all DAGs.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.limit
 * @param data.offset
 * @param data.versionNumber
 * @param data.bundleName
 * @param data.bundleVersion
 * @param data.orderBy
 * @returns DAGVersionCollectionResponse Successful Response
 * @throws ApiError
 */
export const useDagVersionServiceGetDagVersions = <
  TData = Common.DagVersionServiceGetDagVersionsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    bundleName,
    bundleVersion,
    dagId,
    limit,
    offset,
    orderBy,
    versionNumber,
  }: {
    bundleName?: string;
    bundleVersion?: string;
    dagId: string;
    limit?: number;
    offset?: number;
    orderBy?: string;
    versionNumber?: number;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDagVersionServiceGetDagVersionsKeyFn(
      { bundleName, bundleVersion, dagId, limit, offset, orderBy, versionNumber },
      queryKey,
    ),
    queryFn: () =>
      DagVersionService.getDagVersions({
        bundleName,
        bundleVersion,
        dagId,
        limit,
        offset,
        orderBy,
        versionNumber,
      }) as TData,
    ...options,
  });
/**
 * Get Health
 * @returns HealthInfoResponse Successful Response
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
 * Login
 * Redirect to the login URL depending on the AuthManager configured.
 * @param data The data for the request.
 * @param data.next
 * @returns unknown Successful Response
 * @throws ApiError
 */
export const useLoginServiceLogin = <
  TData = Common.LoginServiceLoginDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    next,
  }: {
    next?: string;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseLoginServiceLoginKeyFn({ next }, queryKey),
    queryFn: () => LoginService.login({ next }) as TData,
    ...options,
  });
/**
 * Logout
 * Logout the user.
 * @param data The data for the request.
 * @param data.next
 * @returns unknown Successful Response
 * @throws ApiError
 */
export const useLoginServiceLogout = <
  TData = Common.LoginServiceLogoutDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    next,
  }: {
    next?: string;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseLoginServiceLogoutKeyFn({ next }, queryKey),
    queryFn: () => LoginService.logout({ next }) as TData,
    ...options,
  });
/**
 * Get Auth Menus
 * @returns MenuItemCollectionResponse Successful Response
 * @throws ApiError
 */
export const useAuthLinksServiceGetAuthMenus = <
  TData = Common.AuthLinksServiceGetAuthMenusDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseAuthLinksServiceGetAuthMenusKeyFn(queryKey),
    queryFn: () => AuthLinksService.getAuthMenus() as TData,
    ...options,
  });
/**
 * Get Dependencies
 * Dependencies graph.
 * @param data The data for the request.
 * @param data.nodeId
 * @returns BaseGraphResponse Successful Response
 * @throws ApiError
 */
export const useDependenciesServiceGetDependencies = <
  TData = Common.DependenciesServiceGetDependenciesDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    nodeId,
  }: {
    nodeId?: string;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDependenciesServiceGetDependenciesKeyFn({ nodeId }, queryKey),
    queryFn: () => DependenciesService.getDependencies({ nodeId }) as TData,
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
    queryKey: Common.UseDashboardServiceHistoricalMetricsKeyFn({ endDate, startDate }, queryKey),
    queryFn: () => DashboardService.historicalMetrics({ endDate, startDate }) as TData,
    ...options,
  });
/**
 * Dag Stats
 * Return basic DAG stats with counts of DAGs in various states.
 * @returns DashboardDagStatsResponse Successful Response
 * @throws ApiError
 */
export const useDashboardServiceDagStats = <
  TData = Common.DashboardServiceDagStatsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDashboardServiceDagStatsKeyFn(queryKey),
    queryFn: () => DashboardService.dagStats() as TData,
    ...options,
  });
/**
 * Structure Data
 * Get Structure Data.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.includeUpstream
 * @param data.includeDownstream
 * @param data.root
 * @param data.externalDependencies
 * @param data.versionNumber
 * @returns StructureDataResponse Successful Response
 * @throws ApiError
 */
export const useStructureServiceStructureData = <
  TData = Common.StructureServiceStructureDataDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    externalDependencies,
    includeDownstream,
    includeUpstream,
    root,
    versionNumber,
  }: {
    dagId: string;
    externalDependencies?: boolean;
    includeDownstream?: boolean;
    includeUpstream?: boolean;
    root?: string;
    versionNumber?: number;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseStructureServiceStructureDataKeyFn(
      { dagId, externalDependencies, includeDownstream, includeUpstream, root, versionNumber },
      queryKey,
    ),
    queryFn: () =>
      StructureService.structureData({
        dagId,
        externalDependencies,
        includeDownstream,
        includeUpstream,
        root,
        versionNumber,
      }) as TData,
    ...options,
  });
/**
 * Grid Data
 * Return grid data.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.includeUpstream
 * @param data.includeDownstream
 * @param data.root
 * @param data.offset
 * @param data.runType
 * @param data.state
 * @param data.limit
 * @param data.orderBy
 * @param data.runAfterGte
 * @param data.runAfterLte
 * @param data.logicalDateGte
 * @param data.logicalDateLte
 * @returns GridResponse Successful Response
 * @throws ApiError
 */
export const useGridServiceGridData = <
  TData = Common.GridServiceGridDataDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    includeDownstream,
    includeUpstream,
    limit,
    logicalDateGte,
    logicalDateLte,
    offset,
    orderBy,
    root,
    runAfterGte,
    runAfterLte,
    runType,
    state,
  }: {
    dagId: string;
    includeDownstream?: boolean;
    includeUpstream?: boolean;
    limit?: number;
    logicalDateGte?: string;
    logicalDateLte?: string;
    offset?: number;
    orderBy?: string;
    root?: string;
    runAfterGte?: string;
    runAfterLte?: string;
    runType?: string[];
    state?: string[];
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseGridServiceGridDataKeyFn(
      {
        dagId,
        includeDownstream,
        includeUpstream,
        limit,
        logicalDateGte,
        logicalDateLte,
        offset,
        orderBy,
        root,
        runAfterGte,
        runAfterLte,
        runType,
        state,
      },
      queryKey,
    ),
    queryFn: () =>
      GridService.gridData({
        dagId,
        includeDownstream,
        includeUpstream,
        limit,
        logicalDateGte,
        logicalDateLte,
        offset,
        orderBy,
        root,
        runAfterGte,
        runAfterLte,
        runType,
        state,
      }) as TData,
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
      AssetService.createAssetEvent({ requestBody }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Materialize Asset
 * Materialize an asset by triggering a DAG run that produces it.
 * @param data The data for the request.
 * @param data.assetId
 * @returns DAGRunResponse Successful Response
 * @throws ApiError
 */
export const useAssetServiceMaterializeAsset = <
  TData = Common.AssetServiceMaterializeAssetMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        assetId: number;
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
      assetId: number;
    },
    TContext
  >({
    mutationFn: ({ assetId }) => AssetService.materializeAsset({ assetId }) as unknown as Promise<TData>,
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
      BackfillService.createBackfill({ requestBody }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Create Backfill Dry Run
 * @param data The data for the request.
 * @param data.requestBody
 * @returns DryRunBackfillCollectionResponse Successful Response
 * @throws ApiError
 */
export const useBackfillServiceCreateBackfillDryRun = <
  TData = Common.BackfillServiceCreateBackfillDryRunMutationResult,
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
      BackfillService.createBackfillDryRun({ requestBody }) as unknown as Promise<TData>,
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
      ConnectionService.postConnection({ requestBody }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Test Connection
 * Test an API connection.
 *
 * This method first creates an in-memory transient conn_id & exports that to an env var,
 * as some hook classes tries to find out the `conn` from their __init__ method & errors out if not found.
 * It also deletes the conn id env connection after the test.
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
      ConnectionService.testConnection({ requestBody }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Create Default Connections
 * Create default connections.
 * @returns void Successful Response
 * @throws ApiError
 */
export const useConnectionServiceCreateDefaultConnections = <
  TData = Common.ConnectionServiceCreateDefaultConnectionsMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<UseMutationOptions<TData, TError, void, TContext>, "mutationFn">,
) =>
  useMutation<TData, TError, void, TContext>({
    mutationFn: () => ConnectionService.createDefaultConnections() as unknown as Promise<TData>,
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
      DagRunService.clearDagRun({ dagId, dagRunId, requestBody }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Trigger Dag Run
 * Trigger a DAG.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.requestBody
 * @returns DAGRunResponse Successful Response
 * @throws ApiError
 */
export const useDagRunServiceTriggerDagRun = <
  TData = Common.DagRunServiceTriggerDagRunMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        dagId: unknown;
        requestBody: TriggerDAGRunPostBody;
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
      dagId: unknown;
      requestBody: TriggerDAGRunPostBody;
    },
    TContext
  >({
    mutationFn: ({ dagId, requestBody }) =>
      DagRunService.triggerDagRun({ dagId, requestBody }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Get List Dag Runs Batch
 * Get a list of DAG Runs.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.requestBody
 * @returns DAGRunCollectionResponse Successful Response
 * @throws ApiError
 */
export const useDagRunServiceGetListDagRunsBatch = <
  TData = Common.DagRunServiceGetListDagRunsBatchMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        dagId: "~";
        requestBody: DAGRunsBatchBody;
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
      requestBody: DAGRunsBatchBody;
    },
    TContext
  >({
    mutationFn: ({ dagId, requestBody }) =>
      DagRunService.getListDagRunsBatch({ dagId, requestBody }) as unknown as Promise<TData>,
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
 * Post Clear Task Instances
 * Clear task instances.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.requestBody
 * @returns TaskInstanceCollectionResponse Successful Response
 * @throws ApiError
 */
export const useTaskInstanceServicePostClearTaskInstances = <
  TData = Common.TaskInstanceServicePostClearTaskInstancesMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        dagId: string;
        requestBody: ClearTaskInstancesBody;
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
      requestBody: ClearTaskInstancesBody;
    },
    TContext
  >({
    mutationFn: ({ dagId, requestBody }) =>
      TaskInstanceService.postClearTaskInstances({ dagId, requestBody }) as unknown as Promise<TData>,
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
        requestBody: PoolBody;
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
      requestBody: PoolBody;
    },
    TContext
  >({
    mutationFn: ({ requestBody }) => PoolService.postPool({ requestBody }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Create Xcom Entry
 * Create an XCom entry.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.taskId
 * @param data.dagRunId
 * @param data.requestBody
 * @returns XComResponseNative Successful Response
 * @throws ApiError
 */
export const useXcomServiceCreateXcomEntry = <
  TData = Common.XcomServiceCreateXcomEntryMutationResult,
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
        requestBody: XComCreateBody;
        taskId: string;
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
      requestBody: XComCreateBody;
      taskId: string;
    },
    TContext
  >({
    mutationFn: ({ dagId, dagRunId, requestBody, taskId }) =>
      XcomService.createXcomEntry({ dagId, dagRunId, requestBody, taskId }) as unknown as Promise<TData>,
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
      VariableService.postVariable({ requestBody }) as unknown as Promise<TData>,
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
        backfillId: number;
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
      backfillId: number;
    },
    TContext
  >({
    mutationFn: ({ backfillId }) =>
      BackfillService.pauseBackfill({ backfillId }) as unknown as Promise<TData>,
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
        backfillId: number;
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
      backfillId: number;
    },
    TContext
  >({
    mutationFn: ({ backfillId }) =>
      BackfillService.unpauseBackfill({ backfillId }) as unknown as Promise<TData>,
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
        backfillId: number;
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
      backfillId: number;
    },
    TContext
  >({
    mutationFn: ({ backfillId }) =>
      BackfillService.cancelBackfill({ backfillId }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Reparse Dag File
 * Request re-parsing a DAG file.
 * @param data The data for the request.
 * @param data.fileToken
 * @returns null Successful Response
 * @throws ApiError
 */
export const useDagParsingServiceReparseDagFile = <
  TData = Common.DagParsingServiceReparseDagFileMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        fileToken: string;
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
      fileToken: string;
    },
    TContext
  >({
    mutationFn: ({ fileToken }) =>
      DagParsingService.reparseDagFile({ fileToken }) as unknown as Promise<TData>,
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
 * Bulk Connections
 * Bulk create, update, and delete connections.
 * @param data The data for the request.
 * @param data.requestBody
 * @returns BulkResponse Successful Response
 * @throws ApiError
 */
export const useConnectionServiceBulkConnections = <
  TData = Common.ConnectionServiceBulkConnectionsMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        requestBody: BulkBody_ConnectionBody_;
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
      requestBody: BulkBody_ConnectionBody_;
    },
    TContext
  >({
    mutationFn: ({ requestBody }) =>
      ConnectionService.bulkConnections({ requestBody }) as unknown as Promise<TData>,
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
      DagRunService.patchDagRun({ dagId, dagRunId, requestBody, updateMask }) as unknown as Promise<TData>,
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
 * @param data.tagsMatchMode
 * @param data.owners
 * @param data.dagIdPattern
 * @param data.excludeStale
 * @param data.paused
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
        excludeStale?: boolean;
        limit?: number;
        offset?: number;
        owners?: string[];
        paused?: boolean;
        requestBody: DAGPatchBody;
        tags?: string[];
        tagsMatchMode?: "any" | "all";
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
      excludeStale?: boolean;
      limit?: number;
      offset?: number;
      owners?: string[];
      paused?: boolean;
      requestBody: DAGPatchBody;
      tags?: string[];
      tagsMatchMode?: "any" | "all";
      updateMask?: string[];
    },
    TContext
  >({
    mutationFn: ({
      dagIdPattern,
      excludeStale,
      limit,
      offset,
      owners,
      paused,
      requestBody,
      tags,
      tagsMatchMode,
      updateMask,
    }) =>
      DagService.patchDags({
        dagIdPattern,
        excludeStale,
        limit,
        offset,
        owners,
        paused,
        requestBody,
        tags,
        tagsMatchMode,
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
      DagService.patchDag({ dagId, requestBody, updateMask }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Patch Task Instance
 * Update a task instance.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @param data.taskId
 * @param data.requestBody
 * @param data.mapIndex
 * @param data.updateMask
 * @returns TaskInstanceCollectionResponse Successful Response
 * @throws ApiError
 */
export const useTaskInstanceServicePatchTaskInstance = <
  TData = Common.TaskInstanceServicePatchTaskInstanceMutationResult,
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
        mapIndex?: number;
        requestBody: PatchTaskInstanceBody;
        taskId: string;
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
      mapIndex?: number;
      requestBody: PatchTaskInstanceBody;
      taskId: string;
      updateMask?: string[];
    },
    TContext
  >({
    mutationFn: ({ dagId, dagRunId, mapIndex, requestBody, taskId, updateMask }) =>
      TaskInstanceService.patchTaskInstance({
        dagId,
        dagRunId,
        mapIndex,
        requestBody,
        taskId,
        updateMask,
      }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Patch Task Instance
 * Update a task instance.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @param data.taskId
 * @param data.mapIndex
 * @param data.requestBody
 * @param data.updateMask
 * @returns TaskInstanceCollectionResponse Successful Response
 * @throws ApiError
 */
export const useTaskInstanceServicePatchTaskInstanceByMapIndex = <
  TData = Common.TaskInstanceServicePatchTaskInstanceByMapIndexMutationResult,
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
        mapIndex: number;
        requestBody: PatchTaskInstanceBody;
        taskId: string;
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
      mapIndex: number;
      requestBody: PatchTaskInstanceBody;
      taskId: string;
      updateMask?: string[];
    },
    TContext
  >({
    mutationFn: ({ dagId, dagRunId, mapIndex, requestBody, taskId, updateMask }) =>
      TaskInstanceService.patchTaskInstanceByMapIndex({
        dagId,
        dagRunId,
        mapIndex,
        requestBody,
        taskId,
        updateMask,
      }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Patch Task Instance Dry Run
 * Update a task instance dry_run mode.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @param data.taskId
 * @param data.mapIndex
 * @param data.requestBody
 * @param data.updateMask
 * @returns TaskInstanceCollectionResponse Successful Response
 * @throws ApiError
 */
export const useTaskInstanceServicePatchTaskInstanceDryRunByMapIndex = <
  TData = Common.TaskInstanceServicePatchTaskInstanceDryRunByMapIndexMutationResult,
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
        mapIndex: number;
        requestBody: PatchTaskInstanceBody;
        taskId: string;
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
      mapIndex: number;
      requestBody: PatchTaskInstanceBody;
      taskId: string;
      updateMask?: string[];
    },
    TContext
  >({
    mutationFn: ({ dagId, dagRunId, mapIndex, requestBody, taskId, updateMask }) =>
      TaskInstanceService.patchTaskInstanceDryRunByMapIndex({
        dagId,
        dagRunId,
        mapIndex,
        requestBody,
        taskId,
        updateMask,
      }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Patch Task Instance Dry Run
 * Update a task instance dry_run mode.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @param data.taskId
 * @param data.requestBody
 * @param data.mapIndex
 * @param data.updateMask
 * @returns TaskInstanceCollectionResponse Successful Response
 * @throws ApiError
 */
export const useTaskInstanceServicePatchTaskInstanceDryRun = <
  TData = Common.TaskInstanceServicePatchTaskInstanceDryRunMutationResult,
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
        mapIndex?: number;
        requestBody: PatchTaskInstanceBody;
        taskId: string;
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
      mapIndex?: number;
      requestBody: PatchTaskInstanceBody;
      taskId: string;
      updateMask?: string[];
    },
    TContext
  >({
    mutationFn: ({ dagId, dagRunId, mapIndex, requestBody, taskId, updateMask }) =>
      TaskInstanceService.patchTaskInstanceDryRun({
        dagId,
        dagRunId,
        mapIndex,
        requestBody,
        taskId,
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
      PoolService.patchPool({ poolName, requestBody, updateMask }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Bulk Pools
 * Bulk create, update, and delete pools.
 * @param data The data for the request.
 * @param data.requestBody
 * @returns BulkResponse Successful Response
 * @throws ApiError
 */
export const usePoolServiceBulkPools = <
  TData = Common.PoolServiceBulkPoolsMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        requestBody: BulkBody_PoolBody_;
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
      requestBody: BulkBody_PoolBody_;
    },
    TContext
  >({
    mutationFn: ({ requestBody }) => PoolService.bulkPools({ requestBody }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Update Xcom Entry
 * Update an existing XCom entry.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.taskId
 * @param data.dagRunId
 * @param data.xcomKey
 * @param data.requestBody
 * @returns XComResponseNative Successful Response
 * @throws ApiError
 */
export const useXcomServiceUpdateXcomEntry = <
  TData = Common.XcomServiceUpdateXcomEntryMutationResult,
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
        requestBody: XComUpdateBody;
        taskId: string;
        xcomKey: string;
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
      requestBody: XComUpdateBody;
      taskId: string;
      xcomKey: string;
    },
    TContext
  >({
    mutationFn: ({ dagId, dagRunId, requestBody, taskId, xcomKey }) =>
      XcomService.updateXcomEntry({
        dagId,
        dagRunId,
        requestBody,
        taskId,
        xcomKey,
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
      VariableService.patchVariable({ requestBody, updateMask, variableKey }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Bulk Variables
 * Bulk create, update, and delete variables.
 * @param data The data for the request.
 * @param data.requestBody
 * @returns BulkResponse Successful Response
 * @throws ApiError
 */
export const useVariableServiceBulkVariables = <
  TData = Common.VariableServiceBulkVariablesMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        requestBody: BulkBody_VariableBody_;
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
      requestBody: BulkBody_VariableBody_;
    },
    TContext
  >({
    mutationFn: ({ requestBody }) =>
      VariableService.bulkVariables({ requestBody }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Delete Asset Queued Events
 * Delete queued asset events for an asset.
 * @param data The data for the request.
 * @param data.assetId
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
        assetId: number;
        before?: string;
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
      assetId: number;
      before?: string;
    },
    TContext
  >({
    mutationFn: ({ assetId, before }) =>
      AssetService.deleteAssetQueuedEvents({ assetId, before }) as unknown as Promise<TData>,
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
      AssetService.deleteDagAssetQueuedEvents({ before, dagId }) as unknown as Promise<TData>,
    ...options,
  });
/**
 * Delete Dag Asset Queued Event
 * Delete a queued asset event for a DAG.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.assetId
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
        assetId: number;
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
      assetId: number;
      before?: string;
      dagId: string;
    },
    TContext
  >({
    mutationFn: ({ assetId, before, dagId }) =>
      AssetService.deleteDagAssetQueuedEvent({ assetId, before, dagId }) as unknown as Promise<TData>,
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
      ConnectionService.deleteConnection({ connectionId }) as unknown as Promise<TData>,
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
      DagRunService.deleteDagRun({ dagId, dagRunId }) as unknown as Promise<TData>,
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
  >({ mutationFn: ({ dagId }) => DagService.deleteDag({ dagId }) as unknown as Promise<TData>, ...options });
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
    mutationFn: ({ poolName }) => PoolService.deletePool({ poolName }) as unknown as Promise<TData>,
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
      VariableService.deleteVariable({ variableKey }) as unknown as Promise<TData>,
    ...options,
  });
