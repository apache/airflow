// generated with @7nohe/openapi-react-query-codegen@1.6.2
import { type QueryClient } from "@tanstack/react-query";

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
export const ensureUseAssetServiceGetAssetsData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseAssetServiceGetAssetsKeyFn({
      dagIds,
      limit,
      namePattern,
      offset,
      onlyActive,
      orderBy,
      uriPattern,
    }),
    queryFn: () =>
      AssetService.getAssets({ dagIds, limit, namePattern, offset, onlyActive, orderBy, uriPattern }),
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
export const ensureUseAssetServiceGetAssetAliasesData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseAssetServiceGetAssetAliasesKeyFn({ limit, namePattern, offset, orderBy }),
    queryFn: () => AssetService.getAssetAliases({ limit, namePattern, offset, orderBy }),
  });
/**
 * Get Asset Alias
 * Get an asset alias.
 * @param data The data for the request.
 * @param data.assetAliasId
 * @returns unknown Successful Response
 * @throws ApiError
 */
export const ensureUseAssetServiceGetAssetAliasData = (
  queryClient: QueryClient,
  {
    assetAliasId,
  }: {
    assetAliasId: number;
  },
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseAssetServiceGetAssetAliasKeyFn({ assetAliasId }),
    queryFn: () => AssetService.getAssetAlias({ assetAliasId }),
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
export const ensureUseAssetServiceGetAssetEventsData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseAssetServiceGetAssetEventsKeyFn({
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
    }),
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
      }),
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
export const ensureUseAssetServiceGetAssetQueuedEventsData = (
  queryClient: QueryClient,
  {
    assetId,
    before,
  }: {
    assetId: number;
    before?: string;
  },
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseAssetServiceGetAssetQueuedEventsKeyFn({ assetId, before }),
    queryFn: () => AssetService.getAssetQueuedEvents({ assetId, before }),
  });
/**
 * Get Asset
 * Get an asset.
 * @param data The data for the request.
 * @param data.assetId
 * @returns AssetResponse Successful Response
 * @throws ApiError
 */
export const ensureUseAssetServiceGetAssetData = (
  queryClient: QueryClient,
  {
    assetId,
  }: {
    assetId: number;
  },
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseAssetServiceGetAssetKeyFn({ assetId }),
    queryFn: () => AssetService.getAsset({ assetId }),
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
export const ensureUseAssetServiceGetDagAssetQueuedEventsData = (
  queryClient: QueryClient,
  {
    before,
    dagId,
  }: {
    before?: string;
    dagId: string;
  },
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseAssetServiceGetDagAssetQueuedEventsKeyFn({ before, dagId }),
    queryFn: () => AssetService.getDagAssetQueuedEvents({ before, dagId }),
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
export const ensureUseAssetServiceGetDagAssetQueuedEventData = (
  queryClient: QueryClient,
  {
    assetId,
    before,
    dagId,
  }: {
    assetId: number;
    before?: string;
    dagId: string;
  },
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseAssetServiceGetDagAssetQueuedEventKeyFn({ assetId, before, dagId }),
    queryFn: () => AssetService.getDagAssetQueuedEvent({ assetId, before, dagId }),
  });
/**
 * Next Run Assets
 * @param data The data for the request.
 * @param data.dagId
 * @returns unknown Successful Response
 * @throws ApiError
 */
export const ensureUseAssetServiceNextRunAssetsData = (
  queryClient: QueryClient,
  {
    dagId,
  }: {
    dagId: string;
  },
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseAssetServiceNextRunAssetsKeyFn({ dagId }),
    queryFn: () => AssetService.nextRunAssets({ dagId }),
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
export const ensureUseBackfillServiceListBackfillsData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseBackfillServiceListBackfillsKeyFn({ dagId, limit, offset, orderBy }),
    queryFn: () => BackfillService.listBackfills({ dagId, limit, offset, orderBy }),
  });
/**
 * Get Backfill
 * @param data The data for the request.
 * @param data.backfillId
 * @returns BackfillResponse Successful Response
 * @throws ApiError
 */
export const ensureUseBackfillServiceGetBackfillData = (
  queryClient: QueryClient,
  {
    backfillId,
  }: {
    backfillId: number;
  },
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseBackfillServiceGetBackfillKeyFn({ backfillId }),
    queryFn: () => BackfillService.getBackfill({ backfillId }),
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
export const ensureUseBackfillServiceListBackfills1Data = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseBackfillServiceListBackfills1KeyFn({ active, dagId, limit, offset, orderBy }),
    queryFn: () => BackfillService.listBackfills1({ active, dagId, limit, offset, orderBy }),
  });
/**
 * Get Connection
 * Get a connection entry.
 * @param data The data for the request.
 * @param data.connectionId
 * @returns ConnectionResponse Successful Response
 * @throws ApiError
 */
export const ensureUseConnectionServiceGetConnectionData = (
  queryClient: QueryClient,
  {
    connectionId,
  }: {
    connectionId: string;
  },
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseConnectionServiceGetConnectionKeyFn({ connectionId }),
    queryFn: () => ConnectionService.getConnection({ connectionId }),
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
export const ensureUseConnectionServiceGetConnectionsData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseConnectionServiceGetConnectionsKeyFn({ connectionIdPattern, limit, offset, orderBy }),
    queryFn: () => ConnectionService.getConnections({ connectionIdPattern, limit, offset, orderBy }),
  });
/**
 * Hook Meta Data
 * Retrieve information about available connection types (hook classes) and their parameters.
 * @returns ConnectionHookMetaData Successful Response
 * @throws ApiError
 */
export const ensureUseConnectionServiceHookMetaDataData = (queryClient: QueryClient) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseConnectionServiceHookMetaDataKeyFn(),
    queryFn: () => ConnectionService.hookMetaData(),
  });
/**
 * Get Dag Run
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @returns DAGRunResponse Successful Response
 * @throws ApiError
 */
export const ensureUseDagRunServiceGetDagRunData = (
  queryClient: QueryClient,
  {
    dagId,
    dagRunId,
  }: {
    dagId: string;
    dagRunId: string;
  },
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseDagRunServiceGetDagRunKeyFn({ dagId, dagRunId }),
    queryFn: () => DagRunService.getDagRun({ dagId, dagRunId }),
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
export const ensureUseDagRunServiceGetUpstreamAssetEventsData = (
  queryClient: QueryClient,
  {
    dagId,
    dagRunId,
  }: {
    dagId: string;
    dagRunId: string;
  },
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseDagRunServiceGetUpstreamAssetEventsKeyFn({ dagId, dagRunId }),
    queryFn: () => DagRunService.getUpstreamAssetEvents({ dagId, dagRunId }),
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
export const ensureUseDagRunServiceGetDagRunsData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseDagRunServiceGetDagRunsKeyFn({
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
    }),
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
      }),
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
export const ensureUseDagSourceServiceGetDagSourceData = (
  queryClient: QueryClient,
  {
    accept,
    dagId,
    versionNumber,
  }: {
    accept?: "application/json" | "text/plain" | "*/*";
    dagId: string;
    versionNumber?: number;
  },
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseDagSourceServiceGetDagSourceKeyFn({ accept, dagId, versionNumber }),
    queryFn: () => DagSourceService.getDagSource({ accept, dagId, versionNumber }),
  });
/**
 * Get Dag Stats
 * Get Dag statistics.
 * @param data The data for the request.
 * @param data.dagIds
 * @returns DagStatsCollectionResponse Successful Response
 * @throws ApiError
 */
export const ensureUseDagStatsServiceGetDagStatsData = (
  queryClient: QueryClient,
  {
    dagIds,
  }: {
    dagIds?: string[];
  } = {},
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseDagStatsServiceGetDagStatsKeyFn({ dagIds }),
    queryFn: () => DagStatsService.getDagStats({ dagIds }),
  });
/**
 * Get Dag Reports
 * Get DAG report.
 * @param data The data for the request.
 * @param data.subdir
 * @returns unknown Successful Response
 * @throws ApiError
 */
export const ensureUseDagReportServiceGetDagReportsData = (
  queryClient: QueryClient,
  {
    subdir,
  }: {
    subdir: string;
  },
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseDagReportServiceGetDagReportsKeyFn({ subdir }),
    queryFn: () => DagReportService.getDagReports({ subdir }),
  });
/**
 * Get Config
 * @param data The data for the request.
 * @param data.section
 * @param data.accept
 * @returns Config Successful Response
 * @throws ApiError
 */
export const ensureUseConfigServiceGetConfigData = (
  queryClient: QueryClient,
  {
    accept,
    section,
  }: {
    accept?: "application/json" | "text/plain" | "*/*";
    section?: string;
  } = {},
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseConfigServiceGetConfigKeyFn({ accept, section }),
    queryFn: () => ConfigService.getConfig({ accept, section }),
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
export const ensureUseConfigServiceGetConfigValueData = (
  queryClient: QueryClient,
  {
    accept,
    option,
    section,
  }: {
    accept?: "application/json" | "text/plain" | "*/*";
    option: string;
    section: string;
  },
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseConfigServiceGetConfigValueKeyFn({ accept, option, section }),
    queryFn: () => ConfigService.getConfigValue({ accept, option, section }),
  });
/**
 * Get Configs
 * Get configs for UI.
 * @returns ConfigResponse Successful Response
 * @throws ApiError
 */
export const ensureUseConfigServiceGetConfigsData = (queryClient: QueryClient) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseConfigServiceGetConfigsKeyFn(),
    queryFn: () => ConfigService.getConfigs(),
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
export const ensureUseDagWarningServiceListDagWarningsData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseDagWarningServiceListDagWarningsKeyFn({ dagId, limit, offset, orderBy, warningType }),
    queryFn: () => DagWarningService.listDagWarnings({ dagId, limit, offset, orderBy, warningType }),
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
export const ensureUseDagServiceGetDagsData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseDagServiceGetDagsKeyFn({
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
    }),
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
      }),
  });
/**
 * Get Dag
 * Get basic information about a DAG.
 * @param data The data for the request.
 * @param data.dagId
 * @returns DAGResponse Successful Response
 * @throws ApiError
 */
export const ensureUseDagServiceGetDagData = (
  queryClient: QueryClient,
  {
    dagId,
  }: {
    dagId: string;
  },
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseDagServiceGetDagKeyFn({ dagId }),
    queryFn: () => DagService.getDag({ dagId }),
  });
/**
 * Get Dag Details
 * Get details of DAG.
 * @param data The data for the request.
 * @param data.dagId
 * @returns DAGDetailsResponse Successful Response
 * @throws ApiError
 */
export const ensureUseDagServiceGetDagDetailsData = (
  queryClient: QueryClient,
  {
    dagId,
  }: {
    dagId: string;
  },
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseDagServiceGetDagDetailsKeyFn({ dagId }),
    queryFn: () => DagService.getDagDetails({ dagId }),
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
export const ensureUseDagServiceGetDagTagsData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseDagServiceGetDagTagsKeyFn({ limit, offset, orderBy, tagNamePattern }),
    queryFn: () => DagService.getDagTags({ limit, offset, orderBy, tagNamePattern }),
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
export const ensureUseDagServiceRecentDagRunsData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseDagServiceRecentDagRunsKeyFn({
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
    }),
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
      }),
  });
/**
 * Get Event Log
 * @param data The data for the request.
 * @param data.eventLogId
 * @returns EventLogResponse Successful Response
 * @throws ApiError
 */
export const ensureUseEventLogServiceGetEventLogData = (
  queryClient: QueryClient,
  {
    eventLogId,
  }: {
    eventLogId: number;
  },
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseEventLogServiceGetEventLogKeyFn({ eventLogId }),
    queryFn: () => EventLogService.getEventLog({ eventLogId }),
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
export const ensureUseEventLogServiceGetEventLogsData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseEventLogServiceGetEventLogsKeyFn({
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
    }),
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
      }),
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
export const ensureUseExtraLinksServiceGetExtraLinksData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseExtraLinksServiceGetExtraLinksKeyFn({ dagId, dagRunId, mapIndex, taskId }),
    queryFn: () => ExtraLinksService.getExtraLinks({ dagId, dagRunId, mapIndex, taskId }),
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
export const ensureUseTaskInstanceServiceGetExtraLinksData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseTaskInstanceServiceGetExtraLinksKeyFn({ dagId, dagRunId, mapIndex, taskId }),
    queryFn: () => TaskInstanceService.getExtraLinks({ dagId, dagRunId, mapIndex, taskId }),
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
export const ensureUseTaskInstanceServiceGetTaskInstanceData = (
  queryClient: QueryClient,
  {
    dagId,
    dagRunId,
    taskId,
  }: {
    dagId: string;
    dagRunId: string;
    taskId: string;
  },
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseTaskInstanceServiceGetTaskInstanceKeyFn({ dagId, dagRunId, taskId }),
    queryFn: () => TaskInstanceService.getTaskInstance({ dagId, dagRunId, taskId }),
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
export const ensureUseTaskInstanceServiceGetMappedTaskInstancesData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstancesKeyFn({
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
    }),
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
      }),
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
export const ensureUseTaskInstanceServiceGetTaskInstanceDependenciesByMapIndexData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseTaskInstanceServiceGetTaskInstanceDependenciesByMapIndexKeyFn({
      dagId,
      dagRunId,
      mapIndex,
      taskId,
    }),
    queryFn: () =>
      TaskInstanceService.getTaskInstanceDependenciesByMapIndex({ dagId, dagRunId, mapIndex, taskId }),
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
export const ensureUseTaskInstanceServiceGetTaskInstanceDependenciesData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseTaskInstanceServiceGetTaskInstanceDependenciesKeyFn({
      dagId,
      dagRunId,
      mapIndex,
      taskId,
    }),
    queryFn: () => TaskInstanceService.getTaskInstanceDependencies({ dagId, dagRunId, mapIndex, taskId }),
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
export const ensureUseTaskInstanceServiceGetTaskInstanceTriesData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseTaskInstanceServiceGetTaskInstanceTriesKeyFn({ dagId, dagRunId, mapIndex, taskId }),
    queryFn: () => TaskInstanceService.getTaskInstanceTries({ dagId, dagRunId, mapIndex, taskId }),
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
export const ensureUseTaskInstanceServiceGetMappedTaskInstanceTriesData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstanceTriesKeyFn({
      dagId,
      dagRunId,
      mapIndex,
      taskId,
    }),
    queryFn: () => TaskInstanceService.getMappedTaskInstanceTries({ dagId, dagRunId, mapIndex, taskId }),
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
export const ensureUseTaskInstanceServiceGetMappedTaskInstanceData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstanceKeyFn({ dagId, dagRunId, mapIndex, taskId }),
    queryFn: () => TaskInstanceService.getMappedTaskInstance({ dagId, dagRunId, mapIndex, taskId }),
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
export const ensureUseTaskInstanceServiceGetTaskInstancesData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseTaskInstanceServiceGetTaskInstancesKeyFn({
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
    }),
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
      }),
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
export const ensureUseTaskInstanceServiceGetTaskInstanceTryDetailsData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseTaskInstanceServiceGetTaskInstanceTryDetailsKeyFn({
      dagId,
      dagRunId,
      mapIndex,
      taskId,
      taskTryNumber,
    }),
    queryFn: () =>
      TaskInstanceService.getTaskInstanceTryDetails({ dagId, dagRunId, mapIndex, taskId, taskTryNumber }),
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
export const ensureUseTaskInstanceServiceGetMappedTaskInstanceTryDetailsData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstanceTryDetailsKeyFn({
      dagId,
      dagRunId,
      mapIndex,
      taskId,
      taskTryNumber,
    }),
    queryFn: () =>
      TaskInstanceService.getMappedTaskInstanceTryDetails({
        dagId,
        dagRunId,
        mapIndex,
        taskId,
        taskTryNumber,
      }),
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
export const ensureUseTaskInstanceServiceGetLogData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseTaskInstanceServiceGetLogKeyFn({
      accept,
      dagId,
      dagRunId,
      fullContent,
      mapIndex,
      taskId,
      token,
      tryNumber,
    }),
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
      }),
  });
/**
 * Get Import Error
 * Get an import error.
 * @param data The data for the request.
 * @param data.importErrorId
 * @returns ImportErrorResponse Successful Response
 * @throws ApiError
 */
export const ensureUseImportErrorServiceGetImportErrorData = (
  queryClient: QueryClient,
  {
    importErrorId,
  }: {
    importErrorId: number;
  },
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseImportErrorServiceGetImportErrorKeyFn({ importErrorId }),
    queryFn: () => ImportErrorService.getImportError({ importErrorId }),
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
export const ensureUseImportErrorServiceGetImportErrorsData = (
  queryClient: QueryClient,
  {
    limit,
    offset,
    orderBy,
  }: {
    limit?: number;
    offset?: number;
    orderBy?: string;
  } = {},
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseImportErrorServiceGetImportErrorsKeyFn({ limit, offset, orderBy }),
    queryFn: () => ImportErrorService.getImportErrors({ limit, offset, orderBy }),
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
export const ensureUseJobServiceGetJobsData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseJobServiceGetJobsKeyFn({
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
    }),
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
      }),
  });
/**
 * Get Plugins
 * @param data The data for the request.
 * @param data.limit
 * @param data.offset
 * @returns PluginCollectionResponse Successful Response
 * @throws ApiError
 */
export const ensureUsePluginServiceGetPluginsData = (
  queryClient: QueryClient,
  {
    limit,
    offset,
  }: {
    limit?: number;
    offset?: number;
  } = {},
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UsePluginServiceGetPluginsKeyFn({ limit, offset }),
    queryFn: () => PluginService.getPlugins({ limit, offset }),
  });
/**
 * Get Pool
 * Get a pool.
 * @param data The data for the request.
 * @param data.poolName
 * @returns PoolResponse Successful Response
 * @throws ApiError
 */
export const ensureUsePoolServiceGetPoolData = (
  queryClient: QueryClient,
  {
    poolName,
  }: {
    poolName: string;
  },
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UsePoolServiceGetPoolKeyFn({ poolName }),
    queryFn: () => PoolService.getPool({ poolName }),
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
export const ensureUsePoolServiceGetPoolsData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UsePoolServiceGetPoolsKeyFn({ limit, offset, orderBy, poolNamePattern }),
    queryFn: () => PoolService.getPools({ limit, offset, orderBy, poolNamePattern }),
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
export const ensureUseProviderServiceGetProvidersData = (
  queryClient: QueryClient,
  {
    limit,
    offset,
  }: {
    limit?: number;
    offset?: number;
  } = {},
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseProviderServiceGetProvidersKeyFn({ limit, offset }),
    queryFn: () => ProviderService.getProviders({ limit, offset }),
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
export const ensureUseXcomServiceGetXcomEntryData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseXcomServiceGetXcomEntryKeyFn({
      dagId,
      dagRunId,
      deserialize,
      mapIndex,
      stringify,
      taskId,
      xcomKey,
    }),
    queryFn: () =>
      XcomService.getXcomEntry({ dagId, dagRunId, deserialize, mapIndex, stringify, taskId, xcomKey }),
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
export const ensureUseXcomServiceGetXcomEntriesData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseXcomServiceGetXcomEntriesKeyFn({
      dagId,
      dagRunId,
      limit,
      mapIndex,
      offset,
      taskId,
      xcomKey,
    }),
    queryFn: () => XcomService.getXcomEntries({ dagId, dagRunId, limit, mapIndex, offset, taskId, xcomKey }),
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
export const ensureUseTaskServiceGetTasksData = (
  queryClient: QueryClient,
  {
    dagId,
    orderBy,
  }: {
    dagId: string;
    orderBy?: string;
  },
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseTaskServiceGetTasksKeyFn({ dagId, orderBy }),
    queryFn: () => TaskService.getTasks({ dagId, orderBy }),
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
export const ensureUseTaskServiceGetTaskData = (
  queryClient: QueryClient,
  {
    dagId,
    taskId,
  }: {
    dagId: string;
    taskId: unknown;
  },
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseTaskServiceGetTaskKeyFn({ dagId, taskId }),
    queryFn: () => TaskService.getTask({ dagId, taskId }),
  });
/**
 * Get Variable
 * Get a variable entry.
 * @param data The data for the request.
 * @param data.variableKey
 * @returns VariableResponse Successful Response
 * @throws ApiError
 */
export const ensureUseVariableServiceGetVariableData = (
  queryClient: QueryClient,
  {
    variableKey,
  }: {
    variableKey: string;
  },
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseVariableServiceGetVariableKeyFn({ variableKey }),
    queryFn: () => VariableService.getVariable({ variableKey }),
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
export const ensureUseVariableServiceGetVariablesData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseVariableServiceGetVariablesKeyFn({ limit, offset, orderBy, variableKeyPattern }),
    queryFn: () => VariableService.getVariables({ limit, offset, orderBy, variableKeyPattern }),
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
export const ensureUseDagVersionServiceGetDagVersionData = (
  queryClient: QueryClient,
  {
    dagId,
    versionNumber,
  }: {
    dagId: string;
    versionNumber: number;
  },
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseDagVersionServiceGetDagVersionKeyFn({ dagId, versionNumber }),
    queryFn: () => DagVersionService.getDagVersion({ dagId, versionNumber }),
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
export const ensureUseDagVersionServiceGetDagVersionsData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseDagVersionServiceGetDagVersionsKeyFn({
      bundleName,
      bundleVersion,
      dagId,
      limit,
      offset,
      orderBy,
      versionNumber,
    }),
    queryFn: () =>
      DagVersionService.getDagVersions({
        bundleName,
        bundleVersion,
        dagId,
        limit,
        offset,
        orderBy,
        versionNumber,
      }),
  });
/**
 * Get Health
 * @returns HealthInfoResponse Successful Response
 * @throws ApiError
 */
export const ensureUseMonitorServiceGetHealthData = (queryClient: QueryClient) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseMonitorServiceGetHealthKeyFn(),
    queryFn: () => MonitorService.getHealth(),
  });
/**
 * Get Version
 * Get version information.
 * @returns VersionInfo Successful Response
 * @throws ApiError
 */
export const ensureUseVersionServiceGetVersionData = (queryClient: QueryClient) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseVersionServiceGetVersionKeyFn(),
    queryFn: () => VersionService.getVersion(),
  });
/**
 * Login
 * Redirect to the login URL depending on the AuthManager configured.
 * @param data The data for the request.
 * @param data.next
 * @returns unknown Successful Response
 * @throws ApiError
 */
export const ensureUseLoginServiceLoginData = (
  queryClient: QueryClient,
  {
    next,
  }: {
    next?: string;
  } = {},
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseLoginServiceLoginKeyFn({ next }),
    queryFn: () => LoginService.login({ next }),
  });
/**
 * Logout
 * Logout the user.
 * @param data The data for the request.
 * @param data.next
 * @returns unknown Successful Response
 * @throws ApiError
 */
export const ensureUseLoginServiceLogoutData = (
  queryClient: QueryClient,
  {
    next,
  }: {
    next?: string;
  } = {},
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseLoginServiceLogoutKeyFn({ next }),
    queryFn: () => LoginService.logout({ next }),
  });
/**
 * Get Auth Menus
 * @returns MenuItemCollectionResponse Successful Response
 * @throws ApiError
 */
export const ensureUseAuthLinksServiceGetAuthMenusData = (queryClient: QueryClient) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseAuthLinksServiceGetAuthMenusKeyFn(),
    queryFn: () => AuthLinksService.getAuthMenus(),
  });
/**
 * Get Dependencies
 * Dependencies graph.
 * @param data The data for the request.
 * @param data.nodeId
 * @returns BaseGraphResponse Successful Response
 * @throws ApiError
 */
export const ensureUseDependenciesServiceGetDependenciesData = (
  queryClient: QueryClient,
  {
    nodeId,
  }: {
    nodeId?: string;
  } = {},
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseDependenciesServiceGetDependenciesKeyFn({ nodeId }),
    queryFn: () => DependenciesService.getDependencies({ nodeId }),
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
export const ensureUseDashboardServiceHistoricalMetricsData = (
  queryClient: QueryClient,
  {
    endDate,
    startDate,
  }: {
    endDate?: string;
    startDate: string;
  },
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseDashboardServiceHistoricalMetricsKeyFn({ endDate, startDate }),
    queryFn: () => DashboardService.historicalMetrics({ endDate, startDate }),
  });
/**
 * Dag Stats
 * Return basic DAG stats with counts of DAGs in various states.
 * @returns DashboardDagStatsResponse Successful Response
 * @throws ApiError
 */
export const ensureUseDashboardServiceDagStatsData = (queryClient: QueryClient) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseDashboardServiceDagStatsKeyFn(),
    queryFn: () => DashboardService.dagStats(),
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
export const ensureUseStructureServiceStructureDataData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseStructureServiceStructureDataKeyFn({
      dagId,
      externalDependencies,
      includeDownstream,
      includeUpstream,
      root,
      versionNumber,
    }),
    queryFn: () =>
      StructureService.structureData({
        dagId,
        externalDependencies,
        includeDownstream,
        includeUpstream,
        root,
        versionNumber,
      }),
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
export const ensureUseGridServiceGridDataData = (
  queryClient: QueryClient,
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
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseGridServiceGridDataKeyFn({
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
    }),
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
      }),
  });
