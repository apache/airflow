// generated with @7nohe/openapi-react-query-codegen@1.6.2
import { UseQueryOptions, useSuspenseQuery } from "@tanstack/react-query";

import {
  AssetService,
  AuthLinksService,
  BackfillService,
  ConfigService,
  ConnectionService,
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
import { DagRunState, DagWarningType } from "../requests/types.gen";
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
export const useAssetServiceGetAssetsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useAssetServiceGetAssetAliasesSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useAssetServiceGetAssetAliasSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useAssetServiceGetAssetEventsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useAssetServiceGetAssetQueuedEventsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useAssetServiceGetAssetSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useAssetServiceGetDagAssetQueuedEventsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useAssetServiceGetDagAssetQueuedEventSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
 * List Backfills
 * @param data The data for the request.
 * @param data.dagId
 * @param data.limit
 * @param data.offset
 * @param data.orderBy
 * @returns BackfillCollectionResponse Successful Response
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
export const useBackfillServiceGetBackfillSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useBackfillServiceListBackfills1Suspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useConnectionServiceGetConnectionsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useConnectionServiceHookMetaDataSuspense = <
  TData = Common.ConnectionServiceHookMetaDataDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useSuspenseQuery<TData, TError>({
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
export const useDagRunServiceGetUpstreamAssetEventsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useDagRunServiceGetDagRunsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useDagSourceServiceGetDagSourceSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
 * Get Dag Reports
 * Get DAG report.
 * @param data The data for the request.
 * @param data.subdir
 * @returns unknown Successful Response
 * @throws ApiError
 */
export const useDagReportServiceGetDagReportsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useConfigServiceGetConfigSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useConfigServiceGetConfigValueSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useConfigServiceGetConfigsSuspense = <
  TData = Common.ConfigServiceGetConfigsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useSuspenseQuery<TData, TError>({
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
export const useDagServiceGetDagsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useDagServiceRecentDagRunsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useExtraLinksServiceGetExtraLinksSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useTaskInstanceServiceGetExtraLinksSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useTaskInstanceServiceGetTaskInstanceDependenciesByMapIndexSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
    mapIndex?: number;
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
export const useTaskInstanceServiceGetTaskInstanceTriesSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useTaskInstanceServiceGetMappedTaskInstanceTriesSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useTaskInstanceServiceGetTaskInstanceTryDetailsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useTaskInstanceServiceGetMappedTaskInstanceTryDetailsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useTaskInstanceServiceGetLogSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useJobServiceGetJobsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
 * @param data.poolNamePattern
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
  useSuspenseQuery<TData, TError>({
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
export const useXcomServiceGetXcomEntriesSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useTaskServiceGetTasksSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useTaskServiceGetTaskSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useVariableServiceGetVariablesSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useDagVersionServiceGetDagVersionSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useDagVersionServiceGetDagVersionsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
 * Login
 * Redirect to the login URL depending on the AuthManager configured.
 * @param data The data for the request.
 * @param data.next
 * @returns unknown Successful Response
 * @throws ApiError
 */
export const useLoginServiceLoginSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useLoginServiceLogoutSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseLoginServiceLogoutKeyFn({ next }, queryKey),
    queryFn: () => LoginService.logout({ next }) as TData,
    ...options,
  });
/**
 * Get Auth Menus
 * @returns MenuItemCollectionResponse Successful Response
 * @throws ApiError
 */
export const useAuthLinksServiceGetAuthMenusSuspense = <
  TData = Common.AuthLinksServiceGetAuthMenusDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useSuspenseQuery<TData, TError>({
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
export const useDependenciesServiceGetDependenciesSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useDashboardServiceHistoricalMetricsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useDashboardServiceDagStatsSuspense = <
  TData = Common.DashboardServiceDagStatsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useSuspenseQuery<TData, TError>({
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
export const useStructureServiceStructureDataSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useGridServiceGridDataSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
