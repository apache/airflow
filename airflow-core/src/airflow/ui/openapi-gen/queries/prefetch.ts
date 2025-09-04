// generated with @7nohe/openapi-react-query-codegen@1.6.2 

import { type QueryClient } from "@tanstack/react-query";
import { AssetService, AuthLinksService, BackfillService, CalendarService, ConfigService, ConnectionService, DagReportService, DagRunService, DagService, DagSourceService, DagStatsService, DagVersionService, DagWarningService, DashboardService, DependenciesService, EventLogService, ExperimentalService, ExtraLinksService, GridService, HumanInTheLoopService, ImportErrorService, JobService, LoginService, MonitorService, PluginService, PoolService, ProviderService, StructureService, TaskInstanceService, TaskService, VariableService, VersionService, XcomService } from "../requests/services.gen";
import { DagRunState, DagWarningType } from "../requests/types.gen";
import * as Common from "./common";
/**
* Get Assets
* Get assets.
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @param data.namePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.uriPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.dagIds
* @param data.onlyActive
* @param data.orderBy
* @returns AssetCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseAssetServiceGetAssets = (queryClient: QueryClient, { dagIds, limit, namePattern, offset, onlyActive, orderBy, uriPattern }: {
  dagIds?: string[];
  limit?: number;
  namePattern?: string;
  offset?: number;
  onlyActive?: boolean;
  orderBy?: string[];
  uriPattern?: string;
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseAssetServiceGetAssetsKeyFn({ dagIds, limit, namePattern, offset, onlyActive, orderBy, uriPattern }), queryFn: () => AssetService.getAssets({ dagIds, limit, namePattern, offset, onlyActive, orderBy, uriPattern }) });
/**
* Get Asset Aliases
* Get asset aliases.
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @param data.namePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.orderBy
* @returns AssetAliasCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseAssetServiceGetAssetAliases = (queryClient: QueryClient, { limit, namePattern, offset, orderBy }: {
  limit?: number;
  namePattern?: string;
  offset?: number;
  orderBy?: string[];
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseAssetServiceGetAssetAliasesKeyFn({ limit, namePattern, offset, orderBy }), queryFn: () => AssetService.getAssetAliases({ limit, namePattern, offset, orderBy }) });
/**
* Get Asset Alias
* Get an asset alias.
* @param data The data for the request.
* @param data.assetAliasId
* @returns unknown Successful Response
* @throws ApiError
*/
export const prefetchUseAssetServiceGetAssetAlias = (queryClient: QueryClient, { assetAliasId }: {
  assetAliasId: number;
}) => queryClient.prefetchQuery({ queryKey: Common.UseAssetServiceGetAssetAliasKeyFn({ assetAliasId }), queryFn: () => AssetService.getAssetAlias({ assetAliasId }) });
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
* @param data.timestampGt
* @param data.timestampLte
* @param data.timestampLt
* @returns AssetEventCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseAssetServiceGetAssetEvents = (queryClient: QueryClient, { assetId, limit, offset, orderBy, sourceDagId, sourceMapIndex, sourceRunId, sourceTaskId, timestampGt, timestampGte, timestampLt, timestampLte }: {
  assetId?: number;
  limit?: number;
  offset?: number;
  orderBy?: string[];
  sourceDagId?: string;
  sourceMapIndex?: number;
  sourceRunId?: string;
  sourceTaskId?: string;
  timestampGt?: string;
  timestampGte?: string;
  timestampLt?: string;
  timestampLte?: string;
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseAssetServiceGetAssetEventsKeyFn({ assetId, limit, offset, orderBy, sourceDagId, sourceMapIndex, sourceRunId, sourceTaskId, timestampGt, timestampGte, timestampLt, timestampLte }), queryFn: () => AssetService.getAssetEvents({ assetId, limit, offset, orderBy, sourceDagId, sourceMapIndex, sourceRunId, sourceTaskId, timestampGt, timestampGte, timestampLt, timestampLte }) });
/**
* Get Asset Queued Events
* Get queued asset events for an asset.
* @param data The data for the request.
* @param data.assetId
* @param data.before
* @returns QueuedEventCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseAssetServiceGetAssetQueuedEvents = (queryClient: QueryClient, { assetId, before }: {
  assetId: number;
  before?: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseAssetServiceGetAssetQueuedEventsKeyFn({ assetId, before }), queryFn: () => AssetService.getAssetQueuedEvents({ assetId, before }) });
/**
* Get Asset
* Get an asset.
* @param data The data for the request.
* @param data.assetId
* @returns AssetResponse Successful Response
* @throws ApiError
*/
export const prefetchUseAssetServiceGetAsset = (queryClient: QueryClient, { assetId }: {
  assetId: number;
}) => queryClient.prefetchQuery({ queryKey: Common.UseAssetServiceGetAssetKeyFn({ assetId }), queryFn: () => AssetService.getAsset({ assetId }) });
/**
* Get Dag Asset Queued Events
* Get queued asset events for a DAG.
* @param data The data for the request.
* @param data.dagId
* @param data.before
* @returns QueuedEventCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseAssetServiceGetDagAssetQueuedEvents = (queryClient: QueryClient, { before, dagId }: {
  before?: string;
  dagId: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseAssetServiceGetDagAssetQueuedEventsKeyFn({ before, dagId }), queryFn: () => AssetService.getDagAssetQueuedEvents({ before, dagId }) });
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
export const prefetchUseAssetServiceGetDagAssetQueuedEvent = (queryClient: QueryClient, { assetId, before, dagId }: {
  assetId: number;
  before?: string;
  dagId: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseAssetServiceGetDagAssetQueuedEventKeyFn({ assetId, before, dagId }), queryFn: () => AssetService.getDagAssetQueuedEvent({ assetId, before, dagId }) });
/**
* Next Run Assets
* @param data The data for the request.
* @param data.dagId
* @returns unknown Successful Response
* @throws ApiError
*/
export const prefetchUseAssetServiceNextRunAssets = (queryClient: QueryClient, { dagId }: {
  dagId: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseAssetServiceNextRunAssetsKeyFn({ dagId }), queryFn: () => AssetService.nextRunAssets({ dagId }) });
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
export const prefetchUseBackfillServiceListBackfills = (queryClient: QueryClient, { dagId, limit, offset, orderBy }: {
  dagId: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
}) => queryClient.prefetchQuery({ queryKey: Common.UseBackfillServiceListBackfillsKeyFn({ dagId, limit, offset, orderBy }), queryFn: () => BackfillService.listBackfills({ dagId, limit, offset, orderBy }) });
/**
* Get Backfill
* @param data The data for the request.
* @param data.backfillId
* @returns BackfillResponse Successful Response
* @throws ApiError
*/
export const prefetchUseBackfillServiceGetBackfill = (queryClient: QueryClient, { backfillId }: {
  backfillId: number;
}) => queryClient.prefetchQuery({ queryKey: Common.UseBackfillServiceGetBackfillKeyFn({ backfillId }), queryFn: () => BackfillService.getBackfill({ backfillId }) });
/**
* List Backfills Ui
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @param data.orderBy
* @param data.dagId
* @param data.active
* @returns BackfillCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseBackfillServiceListBackfillsUi = (queryClient: QueryClient, { active, dagId, limit, offset, orderBy }: {
  active?: boolean;
  dagId?: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseBackfillServiceListBackfillsUiKeyFn({ active, dagId, limit, offset, orderBy }), queryFn: () => BackfillService.listBackfillsUi({ active, dagId, limit, offset, orderBy }) });
/**
* Get Connection
* Get a connection entry.
* @param data The data for the request.
* @param data.connectionId
* @returns ConnectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseConnectionServiceGetConnection = (queryClient: QueryClient, { connectionId }: {
  connectionId: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseConnectionServiceGetConnectionKeyFn({ connectionId }), queryFn: () => ConnectionService.getConnection({ connectionId }) });
/**
* Get Connections
* Get all connection entries.
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @param data.orderBy
* @param data.connectionIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @returns ConnectionCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseConnectionServiceGetConnections = (queryClient: QueryClient, { connectionIdPattern, limit, offset, orderBy }: {
  connectionIdPattern?: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseConnectionServiceGetConnectionsKeyFn({ connectionIdPattern, limit, offset, orderBy }), queryFn: () => ConnectionService.getConnections({ connectionIdPattern, limit, offset, orderBy }) });
/**
* Hook Meta Data
* Retrieve information about available connection types (hook classes) and their parameters.
* @returns ConnectionHookMetaData Successful Response
* @throws ApiError
*/
export const prefetchUseConnectionServiceHookMetaData = (queryClient: QueryClient) => queryClient.prefetchQuery({ queryKey: Common.UseConnectionServiceHookMetaDataKeyFn(), queryFn: () => ConnectionService.hookMetaData() });
/**
* Get Dag Run
* @param data The data for the request.
* @param data.dagId
* @param data.dagRunId
* @returns DAGRunResponse Successful Response
* @throws ApiError
*/
export const prefetchUseDagRunServiceGetDagRun = (queryClient: QueryClient, { dagId, dagRunId }: {
  dagId: string;
  dagRunId: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseDagRunServiceGetDagRunKeyFn({ dagId, dagRunId }), queryFn: () => DagRunService.getDagRun({ dagId, dagRunId }) });
/**
* Get Upstream Asset Events
* If dag run is asset-triggered, return the asset events that triggered it.
* @param data The data for the request.
* @param data.dagId
* @param data.dagRunId
* @returns AssetEventCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseDagRunServiceGetUpstreamAssetEvents = (queryClient: QueryClient, { dagId, dagRunId }: {
  dagId: string;
  dagRunId: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseDagRunServiceGetUpstreamAssetEventsKeyFn({ dagId, dagRunId }), queryFn: () => DagRunService.getUpstreamAssetEvents({ dagId, dagRunId }) });
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
* @param data.runAfterGt
* @param data.runAfterLte
* @param data.runAfterLt
* @param data.logicalDateGte
* @param data.logicalDateGt
* @param data.logicalDateLte
* @param data.logicalDateLt
* @param data.startDateGte
* @param data.startDateGt
* @param data.startDateLte
* @param data.startDateLt
* @param data.endDateGte
* @param data.endDateGt
* @param data.endDateLte
* @param data.endDateLt
* @param data.updatedAtGte
* @param data.updatedAtGt
* @param data.updatedAtLte
* @param data.updatedAtLt
* @param data.runType
* @param data.state
* @param data.dagVersion
* @param data.orderBy
* @param data.runIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.triggeringUserNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @returns DAGRunCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseDagRunServiceGetDagRuns = (queryClient: QueryClient, { dagId, dagVersion, endDateGt, endDateGte, endDateLt, endDateLte, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, runType, startDateGt, startDateGte, startDateLt, startDateLte, state, triggeringUserNamePattern, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte }: {
  dagId: string;
  dagVersion?: number[];
  endDateGt?: string;
  endDateGte?: string;
  endDateLt?: string;
  endDateLte?: string;
  limit?: number;
  logicalDateGt?: string;
  logicalDateGte?: string;
  logicalDateLt?: string;
  logicalDateLte?: string;
  offset?: number;
  orderBy?: string[];
  runAfterGt?: string;
  runAfterGte?: string;
  runAfterLt?: string;
  runAfterLte?: string;
  runIdPattern?: string;
  runType?: string[];
  startDateGt?: string;
  startDateGte?: string;
  startDateLt?: string;
  startDateLte?: string;
  state?: string[];
  triggeringUserNamePattern?: string;
  updatedAtGt?: string;
  updatedAtGte?: string;
  updatedAtLt?: string;
  updatedAtLte?: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseDagRunServiceGetDagRunsKeyFn({ dagId, dagVersion, endDateGt, endDateGte, endDateLt, endDateLte, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, runType, startDateGt, startDateGte, startDateLt, startDateLte, state, triggeringUserNamePattern, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte }), queryFn: () => DagRunService.getDagRuns({ dagId, dagVersion, endDateGt, endDateGte, endDateLt, endDateLte, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, runType, startDateGt, startDateGte, startDateLt, startDateLte, state, triggeringUserNamePattern, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte }) });
/**
* Experimental: Wait for a dag run to complete, and return task results if requested.
* 🚧 This is an experimental endpoint and may change or be removed without notice.
* @param data The data for the request.
* @param data.dagId
* @param data.dagRunId
* @param data.interval Seconds to wait between dag run state checks
* @param data.result Collect result XCom from task. Can be set multiple times.
* @returns unknown Successful Response
* @throws ApiError
*/
export const prefetchUseDagRunServiceWaitDagRunUntilFinished = (queryClient: QueryClient, { dagId, dagRunId, interval, result }: {
  dagId: string;
  dagRunId: string;
  interval: number;
  result?: string[];
}) => queryClient.prefetchQuery({ queryKey: Common.UseDagRunServiceWaitDagRunUntilFinishedKeyFn({ dagId, dagRunId, interval, result }), queryFn: () => DagRunService.waitDagRunUntilFinished({ dagId, dagRunId, interval, result }) });
/**
* Experimental: Wait for a dag run to complete, and return task results if requested.
* 🚧 This is an experimental endpoint and may change or be removed without notice.
* @param data The data for the request.
* @param data.dagId
* @param data.dagRunId
* @param data.interval Seconds to wait between dag run state checks
* @param data.result Collect result XCom from task. Can be set multiple times.
* @returns unknown Successful Response
* @throws ApiError
*/
export const prefetchUseExperimentalServiceWaitDagRunUntilFinished = (queryClient: QueryClient, { dagId, dagRunId, interval, result }: {
  dagId: string;
  dagRunId: string;
  interval: number;
  result?: string[];
}) => queryClient.prefetchQuery({ queryKey: Common.UseExperimentalServiceWaitDagRunUntilFinishedKeyFn({ dagId, dagRunId, interval, result }), queryFn: () => ExperimentalService.waitDagRunUntilFinished({ dagId, dagRunId, interval, result }) });
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
export const prefetchUseDagSourceServiceGetDagSource = (queryClient: QueryClient, { accept, dagId, versionNumber }: {
  accept?: "application/json" | "text/plain" | "*/*";
  dagId: string;
  versionNumber?: number;
}) => queryClient.prefetchQuery({ queryKey: Common.UseDagSourceServiceGetDagSourceKeyFn({ accept, dagId, versionNumber }), queryFn: () => DagSourceService.getDagSource({ accept, dagId, versionNumber }) });
/**
* Get Dag Stats
* Get Dag statistics.
* @param data The data for the request.
* @param data.dagIds
* @returns DagStatsCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseDagStatsServiceGetDagStats = (queryClient: QueryClient, { dagIds }: {
  dagIds?: string[];
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseDagStatsServiceGetDagStatsKeyFn({ dagIds }), queryFn: () => DagStatsService.getDagStats({ dagIds }) });
/**
* Get Dag Reports
* Get DAG report.
* @param data The data for the request.
* @param data.subdir
* @returns unknown Successful Response
* @throws ApiError
*/
export const prefetchUseDagReportServiceGetDagReports = (queryClient: QueryClient, { subdir }: {
  subdir: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseDagReportServiceGetDagReportsKeyFn({ subdir }), queryFn: () => DagReportService.getDagReports({ subdir }) });
/**
* Get Config
* @param data The data for the request.
* @param data.section
* @param data.accept
* @returns Config Successful Response
* @throws ApiError
*/
export const prefetchUseConfigServiceGetConfig = (queryClient: QueryClient, { accept, section }: {
  accept?: "application/json" | "text/plain" | "*/*";
  section?: string;
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseConfigServiceGetConfigKeyFn({ accept, section }), queryFn: () => ConfigService.getConfig({ accept, section }) });
/**
* Get Config Value
* @param data The data for the request.
* @param data.section
* @param data.option
* @param data.accept
* @returns Config Successful Response
* @throws ApiError
*/
export const prefetchUseConfigServiceGetConfigValue = (queryClient: QueryClient, { accept, option, section }: {
  accept?: "application/json" | "text/plain" | "*/*";
  option: string;
  section: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseConfigServiceGetConfigValueKeyFn({ accept, option, section }), queryFn: () => ConfigService.getConfigValue({ accept, option, section }) });
/**
* Get Configs
* Get configs for UI.
* @returns ConfigResponse Successful Response
* @throws ApiError
*/
export const prefetchUseConfigServiceGetConfigs = (queryClient: QueryClient) => queryClient.prefetchQuery({ queryKey: Common.UseConfigServiceGetConfigsKeyFn(), queryFn: () => ConfigService.getConfigs() });
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
export const prefetchUseDagWarningServiceListDagWarnings = (queryClient: QueryClient, { dagId, limit, offset, orderBy, warningType }: {
  dagId?: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
  warningType?: DagWarningType;
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseDagWarningServiceListDagWarningsKeyFn({ dagId, limit, offset, orderBy, warningType }), queryFn: () => DagWarningService.listDagWarnings({ dagId, limit, offset, orderBy, warningType }) });
/**
* Get Dags
* Get all DAGs.
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @param data.tags
* @param data.tagsMatchMode
* @param data.owners
* @param data.dagIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.dagDisplayNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.excludeStale
* @param data.paused
* @param data.lastDagRunState
* @param data.bundleName
* @param data.bundleVersion
* @param data.hasAssetSchedule Filter Dags with asset-based scheduling
* @param data.assetDependency Filter Dags by asset dependency (name or URI)
* @param data.dagRunStartDateGte
* @param data.dagRunStartDateGt
* @param data.dagRunStartDateLte
* @param data.dagRunStartDateLt
* @param data.dagRunEndDateGte
* @param data.dagRunEndDateGt
* @param data.dagRunEndDateLte
* @param data.dagRunEndDateLt
* @param data.dagRunState
* @param data.orderBy
* @param data.isFavorite
* @returns DAGCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseDagServiceGetDags = (queryClient: QueryClient, { assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagIdPattern, dagRunEndDateGt, dagRunEndDateGte, dagRunEndDateLt, dagRunEndDateLte, dagRunStartDateGt, dagRunStartDateGte, dagRunStartDateLt, dagRunStartDateLte, dagRunState, excludeStale, hasAssetSchedule, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode }: {
  assetDependency?: string;
  bundleName?: string;
  bundleVersion?: string;
  dagDisplayNamePattern?: string;
  dagIdPattern?: string;
  dagRunEndDateGt?: string;
  dagRunEndDateGte?: string;
  dagRunEndDateLt?: string;
  dagRunEndDateLte?: string;
  dagRunStartDateGt?: string;
  dagRunStartDateGte?: string;
  dagRunStartDateLt?: string;
  dagRunStartDateLte?: string;
  dagRunState?: string[];
  excludeStale?: boolean;
  hasAssetSchedule?: boolean;
  isFavorite?: boolean;
  lastDagRunState?: DagRunState;
  limit?: number;
  offset?: number;
  orderBy?: string[];
  owners?: string[];
  paused?: boolean;
  tags?: string[];
  tagsMatchMode?: "any" | "all";
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseDagServiceGetDagsKeyFn({ assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagIdPattern, dagRunEndDateGt, dagRunEndDateGte, dagRunEndDateLt, dagRunEndDateLte, dagRunStartDateGt, dagRunStartDateGte, dagRunStartDateLt, dagRunStartDateLte, dagRunState, excludeStale, hasAssetSchedule, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode }), queryFn: () => DagService.getDags({ assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagIdPattern, dagRunEndDateGt, dagRunEndDateGte, dagRunEndDateLt, dagRunEndDateLte, dagRunStartDateGt, dagRunStartDateGte, dagRunStartDateLt, dagRunStartDateLte, dagRunState, excludeStale, hasAssetSchedule, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode }) });
/**
* Get Dag
* Get basic information about a DAG.
* @param data The data for the request.
* @param data.dagId
* @returns DAGResponse Successful Response
* @throws ApiError
*/
export const prefetchUseDagServiceGetDag = (queryClient: QueryClient, { dagId }: {
  dagId: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseDagServiceGetDagKeyFn({ dagId }), queryFn: () => DagService.getDag({ dagId }) });
/**
* Get Dag Details
* Get details of DAG.
* @param data The data for the request.
* @param data.dagId
* @returns DAGDetailsResponse Successful Response
* @throws ApiError
*/
export const prefetchUseDagServiceGetDagDetails = (queryClient: QueryClient, { dagId }: {
  dagId: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseDagServiceGetDagDetailsKeyFn({ dagId }), queryFn: () => DagService.getDagDetails({ dagId }) });
/**
* Get Dag Tags
* Get all DAG tags.
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @param data.orderBy
* @param data.tagNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @returns DAGTagCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseDagServiceGetDagTags = (queryClient: QueryClient, { limit, offset, orderBy, tagNamePattern }: {
  limit?: number;
  offset?: number;
  orderBy?: string[];
  tagNamePattern?: string;
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseDagServiceGetDagTagsKeyFn({ limit, offset, orderBy, tagNamePattern }), queryFn: () => DagService.getDagTags({ limit, offset, orderBy, tagNamePattern }) });
/**
* Get Dags
* Get DAGs with recent DagRun.
* @param data The data for the request.
* @param data.dagRunsLimit
* @param data.limit
* @param data.offset
* @param data.tags
* @param data.tagsMatchMode
* @param data.owners
* @param data.dagIds
* @param data.dagIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.dagDisplayNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.excludeStale
* @param data.paused
* @param data.lastDagRunState
* @param data.bundleName
* @param data.bundleVersion
* @param data.orderBy
* @param data.isFavorite
* @param data.hasAssetSchedule Filter Dags with asset-based scheduling
* @param data.assetDependency Filter Dags by asset dependency (name or URI)
* @param data.hasPendingActions
* @returns DAGWithLatestDagRunsCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseDagServiceGetDagsUi = (queryClient: QueryClient, { assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagIdPattern, dagIds, dagRunsLimit, excludeStale, hasAssetSchedule, hasPendingActions, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode }: {
  assetDependency?: string;
  bundleName?: string;
  bundleVersion?: string;
  dagDisplayNamePattern?: string;
  dagIdPattern?: string;
  dagIds?: string[];
  dagRunsLimit?: number;
  excludeStale?: boolean;
  hasAssetSchedule?: boolean;
  hasPendingActions?: boolean;
  isFavorite?: boolean;
  lastDagRunState?: DagRunState;
  limit?: number;
  offset?: number;
  orderBy?: string[];
  owners?: string[];
  paused?: boolean;
  tags?: string[];
  tagsMatchMode?: "any" | "all";
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseDagServiceGetDagsUiKeyFn({ assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagIdPattern, dagIds, dagRunsLimit, excludeStale, hasAssetSchedule, hasPendingActions, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode }), queryFn: () => DagService.getDagsUi({ assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagIdPattern, dagIds, dagRunsLimit, excludeStale, hasAssetSchedule, hasPendingActions, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode }) });
/**
* Get Latest Run Info
* Get latest run.
* @param data The data for the request.
* @param data.dagId
* @returns unknown Successful Response
* @throws ApiError
*/
export const prefetchUseDagServiceGetLatestRunInfo = (queryClient: QueryClient, { dagId }: {
  dagId: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseDagServiceGetLatestRunInfoKeyFn({ dagId }), queryFn: () => DagService.getLatestRunInfo({ dagId }) });
/**
* Get Event Log
* @param data The data for the request.
* @param data.eventLogId
* @returns EventLogResponse Successful Response
* @throws ApiError
*/
export const prefetchUseEventLogServiceGetEventLog = (queryClient: QueryClient, { eventLogId }: {
  eventLogId: number;
}) => queryClient.prefetchQuery({ queryKey: Common.UseEventLogServiceGetEventLogKeyFn({ eventLogId }), queryFn: () => EventLogService.getEventLog({ eventLogId }) });
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
* @param data.dagIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.taskIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.runIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.ownerPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.eventPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @returns EventLogCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseEventLogServiceGetEventLogs = (queryClient: QueryClient, { after, before, dagId, dagIdPattern, event, eventPattern, excludedEvents, includedEvents, limit, mapIndex, offset, orderBy, owner, ownerPattern, runId, runIdPattern, taskId, taskIdPattern, tryNumber }: {
  after?: string;
  before?: string;
  dagId?: string;
  dagIdPattern?: string;
  event?: string;
  eventPattern?: string;
  excludedEvents?: string[];
  includedEvents?: string[];
  limit?: number;
  mapIndex?: number;
  offset?: number;
  orderBy?: string[];
  owner?: string;
  ownerPattern?: string;
  runId?: string;
  runIdPattern?: string;
  taskId?: string;
  taskIdPattern?: string;
  tryNumber?: number;
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseEventLogServiceGetEventLogsKeyFn({ after, before, dagId, dagIdPattern, event, eventPattern, excludedEvents, includedEvents, limit, mapIndex, offset, orderBy, owner, ownerPattern, runId, runIdPattern, taskId, taskIdPattern, tryNumber }), queryFn: () => EventLogService.getEventLogs({ after, before, dagId, dagIdPattern, event, eventPattern, excludedEvents, includedEvents, limit, mapIndex, offset, orderBy, owner, ownerPattern, runId, runIdPattern, taskId, taskIdPattern, tryNumber }) });
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
export const prefetchUseExtraLinksServiceGetExtraLinks = (queryClient: QueryClient, { dagId, dagRunId, mapIndex, taskId }: {
  dagId: string;
  dagRunId: string;
  mapIndex?: number;
  taskId: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseExtraLinksServiceGetExtraLinksKeyFn({ dagId, dagRunId, mapIndex, taskId }), queryFn: () => ExtraLinksService.getExtraLinks({ dagId, dagRunId, mapIndex, taskId }) });
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
export const prefetchUseTaskInstanceServiceGetExtraLinks = (queryClient: QueryClient, { dagId, dagRunId, mapIndex, taskId }: {
  dagId: string;
  dagRunId: string;
  mapIndex?: number;
  taskId: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseTaskInstanceServiceGetExtraLinksKeyFn({ dagId, dagRunId, mapIndex, taskId }), queryFn: () => TaskInstanceService.getExtraLinks({ dagId, dagRunId, mapIndex, taskId }) });
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
export const prefetchUseTaskInstanceServiceGetTaskInstance = (queryClient: QueryClient, { dagId, dagRunId, taskId }: {
  dagId: string;
  dagRunId: string;
  taskId: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseTaskInstanceServiceGetTaskInstanceKeyFn({ dagId, dagRunId, taskId }), queryFn: () => TaskInstanceService.getTaskInstance({ dagId, dagRunId, taskId }) });
/**
* Get Mapped Task Instances
* Get list of mapped task instances.
* @param data The data for the request.
* @param data.dagId
* @param data.dagRunId
* @param data.taskId
* @param data.runAfterGte
* @param data.runAfterGt
* @param data.runAfterLte
* @param data.runAfterLt
* @param data.logicalDateGte
* @param data.logicalDateGt
* @param data.logicalDateLte
* @param data.logicalDateLt
* @param data.startDateGte
* @param data.startDateGt
* @param data.startDateLte
* @param data.startDateLt
* @param data.endDateGte
* @param data.endDateGt
* @param data.endDateLte
* @param data.endDateLt
* @param data.updatedAtGte
* @param data.updatedAtGt
* @param data.updatedAtLte
* @param data.updatedAtLt
* @param data.durationGte
* @param data.durationGt
* @param data.durationLte
* @param data.durationLt
* @param data.state
* @param data.pool
* @param data.queue
* @param data.executor
* @param data.versionNumber
* @param data.tryNumber
* @param data.operator
* @param data.limit
* @param data.offset
* @param data.orderBy
* @returns TaskInstanceCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseTaskInstanceServiceGetMappedTaskInstances = (queryClient: QueryClient, { dagId, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, offset, operator, orderBy, pool, queue, runAfterGt, runAfterGte, runAfterLt, runAfterLte, startDateGt, startDateGte, startDateLt, startDateLte, state, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }: {
  dagId: string;
  dagRunId: string;
  durationGt?: number;
  durationGte?: number;
  durationLt?: number;
  durationLte?: number;
  endDateGt?: string;
  endDateGte?: string;
  endDateLt?: string;
  endDateLte?: string;
  executor?: string[];
  limit?: number;
  logicalDateGt?: string;
  logicalDateGte?: string;
  logicalDateLt?: string;
  logicalDateLte?: string;
  offset?: number;
  operator?: string[];
  orderBy?: string[];
  pool?: string[];
  queue?: string[];
  runAfterGt?: string;
  runAfterGte?: string;
  runAfterLt?: string;
  runAfterLte?: string;
  startDateGt?: string;
  startDateGte?: string;
  startDateLt?: string;
  startDateLte?: string;
  state?: string[];
  taskId: string;
  tryNumber?: number[];
  updatedAtGt?: string;
  updatedAtGte?: string;
  updatedAtLt?: string;
  updatedAtLte?: string;
  versionNumber?: number[];
}) => queryClient.prefetchQuery({ queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstancesKeyFn({ dagId, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, offset, operator, orderBy, pool, queue, runAfterGt, runAfterGte, runAfterLt, runAfterLte, startDateGt, startDateGte, startDateLt, startDateLte, state, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }), queryFn: () => TaskInstanceService.getMappedTaskInstances({ dagId, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, offset, operator, orderBy, pool, queue, runAfterGt, runAfterGte, runAfterLt, runAfterLte, startDateGt, startDateGte, startDateLt, startDateLte, state, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }) });
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
export const prefetchUseTaskInstanceServiceGetTaskInstanceDependenciesByMapIndex = (queryClient: QueryClient, { dagId, dagRunId, mapIndex, taskId }: {
  dagId: string;
  dagRunId: string;
  mapIndex: number;
  taskId: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseTaskInstanceServiceGetTaskInstanceDependenciesByMapIndexKeyFn({ dagId, dagRunId, mapIndex, taskId }), queryFn: () => TaskInstanceService.getTaskInstanceDependenciesByMapIndex({ dagId, dagRunId, mapIndex, taskId }) });
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
export const prefetchUseTaskInstanceServiceGetTaskInstanceDependencies = (queryClient: QueryClient, { dagId, dagRunId, mapIndex, taskId }: {
  dagId: string;
  dagRunId: string;
  mapIndex?: number;
  taskId: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseTaskInstanceServiceGetTaskInstanceDependenciesKeyFn({ dagId, dagRunId, mapIndex, taskId }), queryFn: () => TaskInstanceService.getTaskInstanceDependencies({ dagId, dagRunId, mapIndex, taskId }) });
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
export const prefetchUseTaskInstanceServiceGetTaskInstanceTries = (queryClient: QueryClient, { dagId, dagRunId, mapIndex, taskId }: {
  dagId: string;
  dagRunId: string;
  mapIndex?: number;
  taskId: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseTaskInstanceServiceGetTaskInstanceTriesKeyFn({ dagId, dagRunId, mapIndex, taskId }), queryFn: () => TaskInstanceService.getTaskInstanceTries({ dagId, dagRunId, mapIndex, taskId }) });
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
export const prefetchUseTaskInstanceServiceGetMappedTaskInstanceTries = (queryClient: QueryClient, { dagId, dagRunId, mapIndex, taskId }: {
  dagId: string;
  dagRunId: string;
  mapIndex: number;
  taskId: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstanceTriesKeyFn({ dagId, dagRunId, mapIndex, taskId }), queryFn: () => TaskInstanceService.getMappedTaskInstanceTries({ dagId, dagRunId, mapIndex, taskId }) });
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
export const prefetchUseTaskInstanceServiceGetMappedTaskInstance = (queryClient: QueryClient, { dagId, dagRunId, mapIndex, taskId }: {
  dagId: string;
  dagRunId: string;
  mapIndex: number;
  taskId: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstanceKeyFn({ dagId, dagRunId, mapIndex, taskId }), queryFn: () => TaskInstanceService.getMappedTaskInstance({ dagId, dagRunId, mapIndex, taskId }) });
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
* @param data.runAfterGt
* @param data.runAfterLte
* @param data.runAfterLt
* @param data.logicalDateGte
* @param data.logicalDateGt
* @param data.logicalDateLte
* @param data.logicalDateLt
* @param data.startDateGte
* @param data.startDateGt
* @param data.startDateLte
* @param data.startDateLt
* @param data.endDateGte
* @param data.endDateGt
* @param data.endDateLte
* @param data.endDateLt
* @param data.updatedAtGte
* @param data.updatedAtGt
* @param data.updatedAtLte
* @param data.updatedAtLt
* @param data.durationGte
* @param data.durationGt
* @param data.durationLte
* @param data.durationLt
* @param data.taskDisplayNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.state
* @param data.pool
* @param data.queue
* @param data.executor
* @param data.versionNumber
* @param data.tryNumber
* @param data.operator
* @param data.limit
* @param data.offset
* @param data.orderBy
* @returns TaskInstanceCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseTaskInstanceServiceGetTaskInstances = (queryClient: QueryClient, { dagId, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, offset, operator, orderBy, pool, queue, runAfterGt, runAfterGte, runAfterLt, runAfterLte, startDateGt, startDateGte, startDateLt, startDateLte, state, taskDisplayNamePattern, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }: {
  dagId: string;
  dagRunId: string;
  durationGt?: number;
  durationGte?: number;
  durationLt?: number;
  durationLte?: number;
  endDateGt?: string;
  endDateGte?: string;
  endDateLt?: string;
  endDateLte?: string;
  executor?: string[];
  limit?: number;
  logicalDateGt?: string;
  logicalDateGte?: string;
  logicalDateLt?: string;
  logicalDateLte?: string;
  offset?: number;
  operator?: string[];
  orderBy?: string[];
  pool?: string[];
  queue?: string[];
  runAfterGt?: string;
  runAfterGte?: string;
  runAfterLt?: string;
  runAfterLte?: string;
  startDateGt?: string;
  startDateGte?: string;
  startDateLt?: string;
  startDateLte?: string;
  state?: string[];
  taskDisplayNamePattern?: string;
  taskId?: string;
  tryNumber?: number[];
  updatedAtGt?: string;
  updatedAtGte?: string;
  updatedAtLt?: string;
  updatedAtLte?: string;
  versionNumber?: number[];
}) => queryClient.prefetchQuery({ queryKey: Common.UseTaskInstanceServiceGetTaskInstancesKeyFn({ dagId, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, offset, operator, orderBy, pool, queue, runAfterGt, runAfterGte, runAfterLt, runAfterLte, startDateGt, startDateGte, startDateLt, startDateLte, state, taskDisplayNamePattern, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }), queryFn: () => TaskInstanceService.getTaskInstances({ dagId, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, offset, operator, orderBy, pool, queue, runAfterGt, runAfterGte, runAfterLt, runAfterLte, startDateGt, startDateGte, startDateLt, startDateLte, state, taskDisplayNamePattern, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }) });
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
export const prefetchUseTaskInstanceServiceGetTaskInstanceTryDetails = (queryClient: QueryClient, { dagId, dagRunId, mapIndex, taskId, taskTryNumber }: {
  dagId: string;
  dagRunId: string;
  mapIndex?: number;
  taskId: string;
  taskTryNumber: number;
}) => queryClient.prefetchQuery({ queryKey: Common.UseTaskInstanceServiceGetTaskInstanceTryDetailsKeyFn({ dagId, dagRunId, mapIndex, taskId, taskTryNumber }), queryFn: () => TaskInstanceService.getTaskInstanceTryDetails({ dagId, dagRunId, mapIndex, taskId, taskTryNumber }) });
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
export const prefetchUseTaskInstanceServiceGetMappedTaskInstanceTryDetails = (queryClient: QueryClient, { dagId, dagRunId, mapIndex, taskId, taskTryNumber }: {
  dagId: string;
  dagRunId: string;
  mapIndex: number;
  taskId: string;
  taskTryNumber: number;
}) => queryClient.prefetchQuery({ queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstanceTryDetailsKeyFn({ dagId, dagRunId, mapIndex, taskId, taskTryNumber }), queryFn: () => TaskInstanceService.getMappedTaskInstanceTryDetails({ dagId, dagRunId, mapIndex, taskId, taskTryNumber }) });
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
export const prefetchUseTaskInstanceServiceGetLog = (queryClient: QueryClient, { accept, dagId, dagRunId, fullContent, mapIndex, taskId, token, tryNumber }: {
  accept?: "application/json" | "*/*" | "application/x-ndjson";
  dagId: string;
  dagRunId: string;
  fullContent?: boolean;
  mapIndex?: number;
  taskId: string;
  token?: string;
  tryNumber: number;
}) => queryClient.prefetchQuery({ queryKey: Common.UseTaskInstanceServiceGetLogKeyFn({ accept, dagId, dagRunId, fullContent, mapIndex, taskId, token, tryNumber }), queryFn: () => TaskInstanceService.getLog({ accept, dagId, dagRunId, fullContent, mapIndex, taskId, token, tryNumber }) });
/**
* Get External Log Url
* Get external log URL for a specific task instance.
* @param data The data for the request.
* @param data.dagId
* @param data.dagRunId
* @param data.taskId
* @param data.tryNumber
* @param data.mapIndex
* @returns ExternalLogUrlResponse Successful Response
* @throws ApiError
*/
export const prefetchUseTaskInstanceServiceGetExternalLogUrl = (queryClient: QueryClient, { dagId, dagRunId, mapIndex, taskId, tryNumber }: {
  dagId: string;
  dagRunId: string;
  mapIndex?: number;
  taskId: string;
  tryNumber: number;
}) => queryClient.prefetchQuery({ queryKey: Common.UseTaskInstanceServiceGetExternalLogUrlKeyFn({ dagId, dagRunId, mapIndex, taskId, tryNumber }), queryFn: () => TaskInstanceService.getExternalLogUrl({ dagId, dagRunId, mapIndex, taskId, tryNumber }) });
/**
* Get Import Error
* Get an import error.
* @param data The data for the request.
* @param data.importErrorId
* @returns ImportErrorResponse Successful Response
* @throws ApiError
*/
export const prefetchUseImportErrorServiceGetImportError = (queryClient: QueryClient, { importErrorId }: {
  importErrorId: number;
}) => queryClient.prefetchQuery({ queryKey: Common.UseImportErrorServiceGetImportErrorKeyFn({ importErrorId }), queryFn: () => ImportErrorService.getImportError({ importErrorId }) });
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
export const prefetchUseImportErrorServiceGetImportErrors = (queryClient: QueryClient, { limit, offset, orderBy }: {
  limit?: number;
  offset?: number;
  orderBy?: string[];
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseImportErrorServiceGetImportErrorsKeyFn({ limit, offset, orderBy }), queryFn: () => ImportErrorService.getImportErrors({ limit, offset, orderBy }) });
/**
* Get Jobs
* Get all jobs.
* @param data The data for the request.
* @param data.isAlive
* @param data.startDateGte
* @param data.startDateGt
* @param data.startDateLte
* @param data.startDateLt
* @param data.endDateGte
* @param data.endDateGt
* @param data.endDateLte
* @param data.endDateLt
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
export const prefetchUseJobServiceGetJobs = (queryClient: QueryClient, { endDateGt, endDateGte, endDateLt, endDateLte, executorClass, hostname, isAlive, jobState, jobType, limit, offset, orderBy, startDateGt, startDateGte, startDateLt, startDateLte }: {
  endDateGt?: string;
  endDateGte?: string;
  endDateLt?: string;
  endDateLte?: string;
  executorClass?: string;
  hostname?: string;
  isAlive?: boolean;
  jobState?: string;
  jobType?: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
  startDateGt?: string;
  startDateGte?: string;
  startDateLt?: string;
  startDateLte?: string;
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseJobServiceGetJobsKeyFn({ endDateGt, endDateGte, endDateLt, endDateLte, executorClass, hostname, isAlive, jobState, jobType, limit, offset, orderBy, startDateGt, startDateGte, startDateLt, startDateLte }), queryFn: () => JobService.getJobs({ endDateGt, endDateGte, endDateLt, endDateLte, executorClass, hostname, isAlive, jobState, jobType, limit, offset, orderBy, startDateGt, startDateGte, startDateLt, startDateLte }) });
/**
* Get Plugins
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @returns PluginCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUsePluginServiceGetPlugins = (queryClient: QueryClient, { limit, offset }: {
  limit?: number;
  offset?: number;
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UsePluginServiceGetPluginsKeyFn({ limit, offset }), queryFn: () => PluginService.getPlugins({ limit, offset }) });
/**
* Import Errors
* @returns PluginImportErrorCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUsePluginServiceImportErrors = (queryClient: QueryClient) => queryClient.prefetchQuery({ queryKey: Common.UsePluginServiceImportErrorsKeyFn(), queryFn: () => PluginService.importErrors() });
/**
* Get Pool
* Get a pool.
* @param data The data for the request.
* @param data.poolName
* @returns PoolResponse Successful Response
* @throws ApiError
*/
export const prefetchUsePoolServiceGetPool = (queryClient: QueryClient, { poolName }: {
  poolName: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UsePoolServiceGetPoolKeyFn({ poolName }), queryFn: () => PoolService.getPool({ poolName }) });
/**
* Get Pools
* Get all pools entries.
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @param data.orderBy
* @param data.poolNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @returns PoolCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUsePoolServiceGetPools = (queryClient: QueryClient, { limit, offset, orderBy, poolNamePattern }: {
  limit?: number;
  offset?: number;
  orderBy?: string[];
  poolNamePattern?: string;
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UsePoolServiceGetPoolsKeyFn({ limit, offset, orderBy, poolNamePattern }), queryFn: () => PoolService.getPools({ limit, offset, orderBy, poolNamePattern }) });
/**
* Get Providers
* Get providers.
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @returns ProviderCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseProviderServiceGetProviders = (queryClient: QueryClient, { limit, offset }: {
  limit?: number;
  offset?: number;
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseProviderServiceGetProvidersKeyFn({ limit, offset }), queryFn: () => ProviderService.getProviders({ limit, offset }) });
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
export const prefetchUseXcomServiceGetXcomEntry = (queryClient: QueryClient, { dagId, dagRunId, deserialize, mapIndex, stringify, taskId, xcomKey }: {
  dagId: string;
  dagRunId: string;
  deserialize?: boolean;
  mapIndex?: number;
  stringify?: boolean;
  taskId: string;
  xcomKey: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseXcomServiceGetXcomEntryKeyFn({ dagId, dagRunId, deserialize, mapIndex, stringify, taskId, xcomKey }), queryFn: () => XcomService.getXcomEntry({ dagId, dagRunId, deserialize, mapIndex, stringify, taskId, xcomKey }) });
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
* @param data.xcomKeyPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.dagDisplayNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.runIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.taskIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.mapIndexFilter
* @param data.logicalDateGte
* @param data.logicalDateGt
* @param data.logicalDateLte
* @param data.logicalDateLt
* @param data.runAfterGte
* @param data.runAfterGt
* @param data.runAfterLte
* @param data.runAfterLt
* @returns XComCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseXcomServiceGetXcomEntries = (queryClient: QueryClient, { dagDisplayNamePattern, dagId, dagRunId, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, mapIndexFilter, offset, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, taskId, taskIdPattern, xcomKey, xcomKeyPattern }: {
  dagDisplayNamePattern?: string;
  dagId: string;
  dagRunId: string;
  limit?: number;
  logicalDateGt?: string;
  logicalDateGte?: string;
  logicalDateLt?: string;
  logicalDateLte?: string;
  mapIndex?: number;
  mapIndexFilter?: number;
  offset?: number;
  runAfterGt?: string;
  runAfterGte?: string;
  runAfterLt?: string;
  runAfterLte?: string;
  runIdPattern?: string;
  taskId: string;
  taskIdPattern?: string;
  xcomKey?: string;
  xcomKeyPattern?: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseXcomServiceGetXcomEntriesKeyFn({ dagDisplayNamePattern, dagId, dagRunId, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, mapIndexFilter, offset, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, taskId, taskIdPattern, xcomKey, xcomKeyPattern }), queryFn: () => XcomService.getXcomEntries({ dagDisplayNamePattern, dagId, dagRunId, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, mapIndexFilter, offset, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, taskId, taskIdPattern, xcomKey, xcomKeyPattern }) });
/**
* Get Tasks
* Get tasks for DAG.
* @param data The data for the request.
* @param data.dagId
* @param data.orderBy
* @returns TaskCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseTaskServiceGetTasks = (queryClient: QueryClient, { dagId, orderBy }: {
  dagId: string;
  orderBy?: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseTaskServiceGetTasksKeyFn({ dagId, orderBy }), queryFn: () => TaskService.getTasks({ dagId, orderBy }) });
/**
* Get Task
* Get simplified representation of a task.
* @param data The data for the request.
* @param data.dagId
* @param data.taskId
* @returns TaskResponse Successful Response
* @throws ApiError
*/
export const prefetchUseTaskServiceGetTask = (queryClient: QueryClient, { dagId, taskId }: {
  dagId: string;
  taskId: unknown;
}) => queryClient.prefetchQuery({ queryKey: Common.UseTaskServiceGetTaskKeyFn({ dagId, taskId }), queryFn: () => TaskService.getTask({ dagId, taskId }) });
/**
* Get Variable
* Get a variable entry.
* @param data The data for the request.
* @param data.variableKey
* @returns VariableResponse Successful Response
* @throws ApiError
*/
export const prefetchUseVariableServiceGetVariable = (queryClient: QueryClient, { variableKey }: {
  variableKey: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseVariableServiceGetVariableKeyFn({ variableKey }), queryFn: () => VariableService.getVariable({ variableKey }) });
/**
* Get Variables
* Get all Variables entries.
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @param data.orderBy
* @param data.variableKeyPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @returns VariableCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseVariableServiceGetVariables = (queryClient: QueryClient, { limit, offset, orderBy, variableKeyPattern }: {
  limit?: number;
  offset?: number;
  orderBy?: string[];
  variableKeyPattern?: string;
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseVariableServiceGetVariablesKeyFn({ limit, offset, orderBy, variableKeyPattern }), queryFn: () => VariableService.getVariables({ limit, offset, orderBy, variableKeyPattern }) });
/**
* Get Dag Version
* Get one Dag Version.
* @param data The data for the request.
* @param data.dagId
* @param data.versionNumber
* @returns DagVersionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseDagVersionServiceGetDagVersion = (queryClient: QueryClient, { dagId, versionNumber }: {
  dagId: string;
  versionNumber: number;
}) => queryClient.prefetchQuery({ queryKey: Common.UseDagVersionServiceGetDagVersionKeyFn({ dagId, versionNumber }), queryFn: () => DagVersionService.getDagVersion({ dagId, versionNumber }) });
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
export const prefetchUseDagVersionServiceGetDagVersions = (queryClient: QueryClient, { bundleName, bundleVersion, dagId, limit, offset, orderBy, versionNumber }: {
  bundleName?: string;
  bundleVersion?: string;
  dagId: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
  versionNumber?: number;
}) => queryClient.prefetchQuery({ queryKey: Common.UseDagVersionServiceGetDagVersionsKeyFn({ bundleName, bundleVersion, dagId, limit, offset, orderBy, versionNumber }), queryFn: () => DagVersionService.getDagVersions({ bundleName, bundleVersion, dagId, limit, offset, orderBy, versionNumber }) });
/**
* Get Hitl Detail
* Get a Human-in-the-loop detail of a specific task instance.
* @param data The data for the request.
* @param data.dagId
* @param data.dagRunId
* @param data.taskId
* @param data.mapIndex
* @returns HITLDetail Successful Response
* @throws ApiError
*/
export const prefetchUseHumanInTheLoopServiceGetHitlDetail = (queryClient: QueryClient, { dagId, dagRunId, mapIndex, taskId }: {
  dagId: string;
  dagRunId: string;
  mapIndex?: number;
  taskId: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseHumanInTheLoopServiceGetHitlDetailKeyFn({ dagId, dagRunId, mapIndex, taskId }), queryFn: () => HumanInTheLoopService.getHitlDetail({ dagId, dagRunId, mapIndex, taskId }) });
/**
* Get Hitl Details
* Get Human-in-the-loop details.
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @param data.orderBy
* @param data.dagId
* @param data.dagIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.dagRunId
* @param data.taskId
* @param data.taskIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.state
* @param data.responseReceived
* @param data.respondedUserId
* @param data.respondedUserName
* @param data.subjectSearch SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.bodySearch SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @returns HITLDetailCollection Successful Response
* @throws ApiError
*/
export const prefetchUseHumanInTheLoopServiceGetHitlDetails = (queryClient: QueryClient, { bodySearch, dagId, dagIdPattern, dagRunId, limit, offset, orderBy, respondedUserId, respondedUserName, responseReceived, state, subjectSearch, taskId, taskIdPattern }: {
  bodySearch?: string;
  dagId?: string;
  dagIdPattern?: string;
  dagRunId?: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
  respondedUserId?: string[];
  respondedUserName?: string[];
  responseReceived?: boolean;
  state?: string[];
  subjectSearch?: string;
  taskId?: string;
  taskIdPattern?: string;
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseHumanInTheLoopServiceGetHitlDetailsKeyFn({ bodySearch, dagId, dagIdPattern, dagRunId, limit, offset, orderBy, respondedUserId, respondedUserName, responseReceived, state, subjectSearch, taskId, taskIdPattern }), queryFn: () => HumanInTheLoopService.getHitlDetails({ bodySearch, dagId, dagIdPattern, dagRunId, limit, offset, orderBy, respondedUserId, respondedUserName, responseReceived, state, subjectSearch, taskId, taskIdPattern }) });
/**
* Get Health
* @returns HealthInfoResponse Successful Response
* @throws ApiError
*/
export const prefetchUseMonitorServiceGetHealth = (queryClient: QueryClient) => queryClient.prefetchQuery({ queryKey: Common.UseMonitorServiceGetHealthKeyFn(), queryFn: () => MonitorService.getHealth() });
/**
* Get Version
* Get version information.
* @returns VersionInfo Successful Response
* @throws ApiError
*/
export const prefetchUseVersionServiceGetVersion = (queryClient: QueryClient) => queryClient.prefetchQuery({ queryKey: Common.UseVersionServiceGetVersionKeyFn(), queryFn: () => VersionService.getVersion() });
/**
* Login
* Redirect to the login URL depending on the AuthManager configured.
* @param data The data for the request.
* @param data.next
* @returns unknown Successful Response
* @throws ApiError
*/
export const prefetchUseLoginServiceLogin = (queryClient: QueryClient, { next }: {
  next?: string;
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseLoginServiceLoginKeyFn({ next }), queryFn: () => LoginService.login({ next }) });
/**
* Logout
* Logout the user.
* @param data The data for the request.
* @param data.next
* @returns unknown Successful Response
* @throws ApiError
*/
export const prefetchUseLoginServiceLogout = (queryClient: QueryClient, { next }: {
  next?: string;
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseLoginServiceLogoutKeyFn({ next }), queryFn: () => LoginService.logout({ next }) });
/**
* Refresh
* Refresh the authentication token.
* @param data The data for the request.
* @param data.next
* @returns unknown Successful Response
* @throws ApiError
*/
export const prefetchUseLoginServiceRefresh = (queryClient: QueryClient, { next }: {
  next?: string;
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseLoginServiceRefreshKeyFn({ next }), queryFn: () => LoginService.refresh({ next }) });
/**
* Get Auth Menus
* @returns MenuItemCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseAuthLinksServiceGetAuthMenus = (queryClient: QueryClient) => queryClient.prefetchQuery({ queryKey: Common.UseAuthLinksServiceGetAuthMenusKeyFn(), queryFn: () => AuthLinksService.getAuthMenus() });
/**
* Get Dependencies
* Dependencies graph.
* @param data The data for the request.
* @param data.nodeId
* @returns BaseGraphResponse Successful Response
* @throws ApiError
*/
export const prefetchUseDependenciesServiceGetDependencies = (queryClient: QueryClient, { nodeId }: {
  nodeId?: string;
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseDependenciesServiceGetDependenciesKeyFn({ nodeId }), queryFn: () => DependenciesService.getDependencies({ nodeId }) });
/**
* Historical Metrics
* Return cluster activity historical metrics.
* @param data The data for the request.
* @param data.startDate
* @param data.endDate
* @returns HistoricalMetricDataResponse Successful Response
* @throws ApiError
*/
export const prefetchUseDashboardServiceHistoricalMetrics = (queryClient: QueryClient, { endDate, startDate }: {
  endDate?: string;
  startDate: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseDashboardServiceHistoricalMetricsKeyFn({ endDate, startDate }), queryFn: () => DashboardService.historicalMetrics({ endDate, startDate }) });
/**
* Dag Stats
* Return basic DAG stats with counts of DAGs in various states.
* @returns DashboardDagStatsResponse Successful Response
* @throws ApiError
*/
export const prefetchUseDashboardServiceDagStats = (queryClient: QueryClient) => queryClient.prefetchQuery({ queryKey: Common.UseDashboardServiceDagStatsKeyFn(), queryFn: () => DashboardService.dagStats() });
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
export const prefetchUseStructureServiceStructureData = (queryClient: QueryClient, { dagId, externalDependencies, includeDownstream, includeUpstream, root, versionNumber }: {
  dagId: string;
  externalDependencies?: boolean;
  includeDownstream?: boolean;
  includeUpstream?: boolean;
  root?: string;
  versionNumber?: number;
}) => queryClient.prefetchQuery({ queryKey: Common.UseStructureServiceStructureDataKeyFn({ dagId, externalDependencies, includeDownstream, includeUpstream, root, versionNumber }), queryFn: () => StructureService.structureData({ dagId, externalDependencies, includeDownstream, includeUpstream, root, versionNumber }) });
/**
* Get Dag Structure
* Return dag structure for grid view.
* @param data The data for the request.
* @param data.dagId
* @param data.offset
* @param data.limit
* @param data.orderBy
* @param data.runAfterGte
* @param data.runAfterGt
* @param data.runAfterLte
* @param data.runAfterLt
* @returns GridNodeResponse Successful Response
* @throws ApiError
*/
export const prefetchUseGridServiceGetDagStructure = (queryClient: QueryClient, { dagId, limit, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte }: {
  dagId: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
  runAfterGt?: string;
  runAfterGte?: string;
  runAfterLt?: string;
  runAfterLte?: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseGridServiceGetDagStructureKeyFn({ dagId, limit, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte }), queryFn: () => GridService.getDagStructure({ dagId, limit, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte }) });
/**
* Get Grid Runs
* Get info about a run for the grid.
* @param data The data for the request.
* @param data.dagId
* @param data.offset
* @param data.limit
* @param data.orderBy
* @param data.runAfterGte
* @param data.runAfterGt
* @param data.runAfterLte
* @param data.runAfterLt
* @returns GridRunsResponse Successful Response
* @throws ApiError
*/
export const prefetchUseGridServiceGetGridRuns = (queryClient: QueryClient, { dagId, limit, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte }: {
  dagId: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
  runAfterGt?: string;
  runAfterGte?: string;
  runAfterLt?: string;
  runAfterLte?: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseGridServiceGetGridRunsKeyFn({ dagId, limit, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte }), queryFn: () => GridService.getGridRuns({ dagId, limit, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte }) });
/**
* Get Grid Ti Summaries
* Get states for TIs / "groups" of TIs.
*
* Essentially this is to know what color to put in the squares in the grid.
*
* The tricky part here is that we aggregate the state for groups and mapped tasks.
*
* We don't add all the TIs for mapped TIs -- we only add one entry for the mapped task and
* its state is an aggregate of its TI states.
*
* And for task groups, we add a "task" for that which is not really a task but is just
* an entry that represents the group (so that we can show a filled in box when the group
* is not expanded) and its state is an agg of those within it.
* @param data The data for the request.
* @param data.dagId
* @param data.runId
* @returns GridTISummaries Successful Response
* @throws ApiError
*/
export const prefetchUseGridServiceGetGridTiSummaries = (queryClient: QueryClient, { dagId, runId }: {
  dagId: string;
  runId: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseGridServiceGetGridTiSummariesKeyFn({ dagId, runId }), queryFn: () => GridService.getGridTiSummaries({ dagId, runId }) });
/**
* Get Calendar
* Get calendar data for a DAG including historical and planned DAG runs.
* @param data The data for the request.
* @param data.dagId
* @param data.granularity
* @param data.logicalDateGte
* @param data.logicalDateGt
* @param data.logicalDateLte
* @param data.logicalDateLt
* @returns CalendarTimeRangeCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseCalendarServiceGetCalendar = (queryClient: QueryClient, { dagId, granularity, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte }: {
  dagId: string;
  granularity?: "hourly" | "daily";
  logicalDateGt?: string;
  logicalDateGte?: string;
  logicalDateLt?: string;
  logicalDateLte?: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseCalendarServiceGetCalendarKeyFn({ dagId, granularity, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte }), queryFn: () => CalendarService.getCalendar({ dagId, granularity, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte }) });
