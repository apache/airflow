// generated with @7nohe/openapi-react-query-codegen@1.6.2 

import { type QueryClient } from "@tanstack/react-query";
import { AssetService, AuthLinksService, BackfillService, CalendarService, ConfigService, ConnectionService, DagRunService, DagService, DagSourceService, DagStatsService, DagVersionService, DagWarningService, DashboardService, DependenciesService, EventLogService, ExperimentalService, ExtraLinksService, GridService, ImportErrorService, JobService, LoginService, MonitorService, PluginService, PoolService, ProviderService, StructureService, TaskInstanceService, TaskService, TeamsService, VariableService, VersionService, XcomService } from "../requests/services.gen";
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
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `id, name, uri, created_at, updated_at`
* @returns AssetCollectionResponse Successful Response
* @throws ApiError
*/
export const ensureUseAssetServiceGetAssetsData = (queryClient: QueryClient, { dagIds, limit, namePattern, offset, onlyActive, orderBy, uriPattern }: {
  dagIds?: string[];
  limit?: number;
  namePattern?: string;
  offset?: number;
  onlyActive?: boolean;
  orderBy?: string[];
  uriPattern?: string;
} = {}) => queryClient.ensureQueryData({ queryKey: Common.UseAssetServiceGetAssetsKeyFn({ dagIds, limit, namePattern, offset, onlyActive, orderBy, uriPattern }), queryFn: () => AssetService.getAssets({ dagIds, limit, namePattern, offset, onlyActive, orderBy, uriPattern }) });
/**
* Get Asset Aliases
* Get asset aliases.
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @param data.namePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `id, name`
* @returns AssetAliasCollectionResponse Successful Response
* @throws ApiError
*/
export const ensureUseAssetServiceGetAssetAliasesData = (queryClient: QueryClient, { limit, namePattern, offset, orderBy }: {
  limit?: number;
  namePattern?: string;
  offset?: number;
  orderBy?: string[];
} = {}) => queryClient.ensureQueryData({ queryKey: Common.UseAssetServiceGetAssetAliasesKeyFn({ limit, namePattern, offset, orderBy }), queryFn: () => AssetService.getAssetAliases({ limit, namePattern, offset, orderBy }) });
/**
* Get Asset Alias
* Get an asset alias.
* @param data The data for the request.
* @param data.assetAliasId
* @returns unknown Successful Response
* @throws ApiError
*/
export const ensureUseAssetServiceGetAssetAliasData = (queryClient: QueryClient, { assetAliasId }: {
  assetAliasId: number;
}) => queryClient.ensureQueryData({ queryKey: Common.UseAssetServiceGetAssetAliasKeyFn({ assetAliasId }), queryFn: () => AssetService.getAssetAlias({ assetAliasId }) });
/**
* Get Asset Events
* Get asset events.
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `source_task_id, source_dag_id, source_run_id, source_map_index, timestamp`
* @param data.assetId
* @param data.sourceDagId
* @param data.sourceTaskId
* @param data.sourceRunId
* @param data.sourceMapIndex
* @param data.namePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.timestampGte
* @param data.timestampGt
* @param data.timestampLte
* @param data.timestampLt
* @returns AssetEventCollectionResponse Successful Response
* @throws ApiError
*/
export const ensureUseAssetServiceGetAssetEventsData = (queryClient: QueryClient, { assetId, limit, namePattern, offset, orderBy, sourceDagId, sourceMapIndex, sourceRunId, sourceTaskId, timestampGt, timestampGte, timestampLt, timestampLte }: {
  assetId?: number;
  limit?: number;
  namePattern?: string;
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
} = {}) => queryClient.ensureQueryData({ queryKey: Common.UseAssetServiceGetAssetEventsKeyFn({ assetId, limit, namePattern, offset, orderBy, sourceDagId, sourceMapIndex, sourceRunId, sourceTaskId, timestampGt, timestampGte, timestampLt, timestampLte }), queryFn: () => AssetService.getAssetEvents({ assetId, limit, namePattern, offset, orderBy, sourceDagId, sourceMapIndex, sourceRunId, sourceTaskId, timestampGt, timestampGte, timestampLt, timestampLte }) });
/**
* Get Asset Queued Events
* Get queued asset events for an asset.
* @param data The data for the request.
* @param data.assetId
* @param data.before
* @returns QueuedEventCollectionResponse Successful Response
* @throws ApiError
*/
export const ensureUseAssetServiceGetAssetQueuedEventsData = (queryClient: QueryClient, { assetId, before }: {
  assetId: number;
  before?: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseAssetServiceGetAssetQueuedEventsKeyFn({ assetId, before }), queryFn: () => AssetService.getAssetQueuedEvents({ assetId, before }) });
/**
* Get Asset
* Get an asset.
* @param data The data for the request.
* @param data.assetId
* @returns AssetResponse Successful Response
* @throws ApiError
*/
export const ensureUseAssetServiceGetAssetData = (queryClient: QueryClient, { assetId }: {
  assetId: number;
}) => queryClient.ensureQueryData({ queryKey: Common.UseAssetServiceGetAssetKeyFn({ assetId }), queryFn: () => AssetService.getAsset({ assetId }) });
/**
* Get Dag Asset Queued Events
* Get queued asset events for a DAG.
* @param data The data for the request.
* @param data.dagId
* @param data.before
* @returns QueuedEventCollectionResponse Successful Response
* @throws ApiError
*/
export const ensureUseAssetServiceGetDagAssetQueuedEventsData = (queryClient: QueryClient, { before, dagId }: {
  before?: string;
  dagId: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseAssetServiceGetDagAssetQueuedEventsKeyFn({ before, dagId }), queryFn: () => AssetService.getDagAssetQueuedEvents({ before, dagId }) });
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
export const ensureUseAssetServiceGetDagAssetQueuedEventData = (queryClient: QueryClient, { assetId, before, dagId }: {
  assetId: number;
  before?: string;
  dagId: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseAssetServiceGetDagAssetQueuedEventKeyFn({ assetId, before, dagId }), queryFn: () => AssetService.getDagAssetQueuedEvent({ assetId, before, dagId }) });
/**
* Next Run Assets
* @param data The data for the request.
* @param data.dagId
* @returns unknown Successful Response
* @throws ApiError
*/
export const ensureUseAssetServiceNextRunAssetsData = (queryClient: QueryClient, { dagId }: {
  dagId: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseAssetServiceNextRunAssetsKeyFn({ dagId }), queryFn: () => AssetService.nextRunAssets({ dagId }) });
/**
* List Backfills
* @param data The data for the request.
* @param data.dagId
* @param data.limit
* @param data.offset
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `id`
* @returns BackfillCollectionResponse Successful Response
* @throws ApiError
*/
export const ensureUseBackfillServiceListBackfillsData = (queryClient: QueryClient, { dagId, limit, offset, orderBy }: {
  dagId: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
}) => queryClient.ensureQueryData({ queryKey: Common.UseBackfillServiceListBackfillsKeyFn({ dagId, limit, offset, orderBy }), queryFn: () => BackfillService.listBackfills({ dagId, limit, offset, orderBy }) });
/**
* Get Backfill
* @param data The data for the request.
* @param data.backfillId
* @returns BackfillResponse Successful Response
* @throws ApiError
*/
export const ensureUseBackfillServiceGetBackfillData = (queryClient: QueryClient, { backfillId }: {
  backfillId: number;
}) => queryClient.ensureQueryData({ queryKey: Common.UseBackfillServiceGetBackfillKeyFn({ backfillId }), queryFn: () => BackfillService.getBackfill({ backfillId }) });
/**
* List Backfills Ui
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `id`
* @param data.dagId
* @param data.active
* @returns BackfillCollectionResponse Successful Response
* @throws ApiError
*/
export const ensureUseBackfillServiceListBackfillsUiData = (queryClient: QueryClient, { active, dagId, limit, offset, orderBy }: {
  active?: boolean;
  dagId?: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
} = {}) => queryClient.ensureQueryData({ queryKey: Common.UseBackfillServiceListBackfillsUiKeyFn({ active, dagId, limit, offset, orderBy }), queryFn: () => BackfillService.listBackfillsUi({ active, dagId, limit, offset, orderBy }) });
/**
* Get Connection
* Get a connection entry.
* @param data The data for the request.
* @param data.connectionId
* @returns ConnectionResponse Successful Response
* @throws ApiError
*/
export const ensureUseConnectionServiceGetConnectionData = (queryClient: QueryClient, { connectionId }: {
  connectionId: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseConnectionServiceGetConnectionKeyFn({ connectionId }), queryFn: () => ConnectionService.getConnection({ connectionId }) });
/**
* Get Connections
* Get all connection entries.
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `conn_id, conn_type, description, host, port, id, connection_id`
* @param data.connectionIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @returns ConnectionCollectionResponse Successful Response
* @throws ApiError
*/
export const ensureUseConnectionServiceGetConnectionsData = (queryClient: QueryClient, { connectionIdPattern, limit, offset, orderBy }: {
  connectionIdPattern?: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
} = {}) => queryClient.ensureQueryData({ queryKey: Common.UseConnectionServiceGetConnectionsKeyFn({ connectionIdPattern, limit, offset, orderBy }), queryFn: () => ConnectionService.getConnections({ connectionIdPattern, limit, offset, orderBy }) });
/**
* Hook Meta Data
* Retrieve information about available connection types (hook classes) and their parameters.
* @returns ConnectionHookMetaData Successful Response
* @throws ApiError
*/
export const ensureUseConnectionServiceHookMetaDataData = (queryClient: QueryClient) => queryClient.ensureQueryData({ queryKey: Common.UseConnectionServiceHookMetaDataKeyFn(), queryFn: () => ConnectionService.hookMetaData() });
/**
* Get Dag Run
* @param data The data for the request.
* @param data.dagId
* @param data.dagRunId
* @returns DAGRunResponse Successful Response
* @throws ApiError
*/
export const ensureUseDagRunServiceGetDagRunData = (queryClient: QueryClient, { dagId, dagRunId }: {
  dagId: string;
  dagRunId: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseDagRunServiceGetDagRunKeyFn({ dagId, dagRunId }), queryFn: () => DagRunService.getDagRun({ dagId, dagRunId }) });
/**
* Get Upstream Asset Events
* If dag run is asset-triggered, return the asset events that triggered it.
* @param data The data for the request.
* @param data.dagId
* @param data.dagRunId
* @returns AssetEventCollectionResponse Successful Response
* @throws ApiError
*/
export const ensureUseDagRunServiceGetUpstreamAssetEventsData = (queryClient: QueryClient, { dagId, dagRunId }: {
  dagId: string;
  dagRunId: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseDagRunServiceGetUpstreamAssetEventsKeyFn({ dagId, dagRunId }), queryFn: () => DagRunService.getUpstreamAssetEvents({ dagId, dagRunId }) });
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
* @param data.durationGte
* @param data.durationGt
* @param data.durationLte
* @param data.durationLt
* @param data.updatedAtGte
* @param data.updatedAtGt
* @param data.updatedAtLte
* @param data.updatedAtLt
* @param data.confContains
* @param data.runType
* @param data.state
* @param data.dagVersion
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `id, state, dag_id, run_id, logical_date, run_after, start_date, end_date, updated_at, conf, duration, dag_run_id`
* @param data.runIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.triggeringUserNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.dagIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @returns DAGRunCollectionResponse Successful Response
* @throws ApiError
*/
export const ensureUseDagRunServiceGetDagRunsData = (queryClient: QueryClient, { confContains, dagId, dagIdPattern, dagVersion, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, runType, startDateGt, startDateGte, startDateLt, startDateLte, state, triggeringUserNamePattern, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte }: {
  confContains?: string;
  dagId: string;
  dagIdPattern?: string;
  dagVersion?: number[];
  durationGt?: number;
  durationGte?: number;
  durationLt?: number;
  durationLte?: number;
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
}) => queryClient.ensureQueryData({ queryKey: Common.UseDagRunServiceGetDagRunsKeyFn({ confContains, dagId, dagIdPattern, dagVersion, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, runType, startDateGt, startDateGte, startDateLt, startDateLte, state, triggeringUserNamePattern, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte }), queryFn: () => DagRunService.getDagRuns({ confContains, dagId, dagIdPattern, dagVersion, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, runType, startDateGt, startDateGte, startDateLt, startDateLte, state, triggeringUserNamePattern, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte }) });
/**
* Experimental: Wait for a dag run to complete, and return task results if requested.
* 🚧 This is an experimental endpoint and may change or be removed without notice.Successful response are streamed as newline-delimited JSON (NDJSON). Each line is a JSON object representing the DAG run state.
* @param data The data for the request.
* @param data.dagId
* @param data.dagRunId
* @param data.interval Seconds to wait between dag run state checks
* @param data.result Collect result XCom from task. Can be set multiple times.
* @returns unknown Successful Response
* @throws ApiError
*/
export const ensureUseDagRunServiceWaitDagRunUntilFinishedData = (queryClient: QueryClient, { dagId, dagRunId, interval, result }: {
  dagId: string;
  dagRunId: string;
  interval: number;
  result?: string[];
}) => queryClient.ensureQueryData({ queryKey: Common.UseDagRunServiceWaitDagRunUntilFinishedKeyFn({ dagId, dagRunId, interval, result }), queryFn: () => DagRunService.waitDagRunUntilFinished({ dagId, dagRunId, interval, result }) });
/**
* Experimental: Wait for a dag run to complete, and return task results if requested.
* 🚧 This is an experimental endpoint and may change or be removed without notice.Successful response are streamed as newline-delimited JSON (NDJSON). Each line is a JSON object representing the DAG run state.
* @param data The data for the request.
* @param data.dagId
* @param data.dagRunId
* @param data.interval Seconds to wait between dag run state checks
* @param data.result Collect result XCom from task. Can be set multiple times.
* @returns unknown Successful Response
* @throws ApiError
*/
export const ensureUseExperimentalServiceWaitDagRunUntilFinishedData = (queryClient: QueryClient, { dagId, dagRunId, interval, result }: {
  dagId: string;
  dagRunId: string;
  interval: number;
  result?: string[];
}) => queryClient.ensureQueryData({ queryKey: Common.UseExperimentalServiceWaitDagRunUntilFinishedKeyFn({ dagId, dagRunId, interval, result }), queryFn: () => ExperimentalService.waitDagRunUntilFinished({ dagId, dagRunId, interval, result }) });
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
export const ensureUseDagSourceServiceGetDagSourceData = (queryClient: QueryClient, { accept, dagId, versionNumber }: {
  accept?: "application/json" | "text/plain" | "*/*";
  dagId: string;
  versionNumber?: number;
}) => queryClient.ensureQueryData({ queryKey: Common.UseDagSourceServiceGetDagSourceKeyFn({ accept, dagId, versionNumber }), queryFn: () => DagSourceService.getDagSource({ accept, dagId, versionNumber }) });
/**
* Get Dag Stats
* Get Dag statistics.
* @param data The data for the request.
* @param data.dagIds
* @returns DagStatsCollectionResponse Successful Response
* @throws ApiError
*/
export const ensureUseDagStatsServiceGetDagStatsData = (queryClient: QueryClient, { dagIds }: {
  dagIds?: string[];
} = {}) => queryClient.ensureQueryData({ queryKey: Common.UseDagStatsServiceGetDagStatsKeyFn({ dagIds }), queryFn: () => DagStatsService.getDagStats({ dagIds }) });
/**
* Get Config
* @param data The data for the request.
* @param data.section
* @param data.accept
* @returns Config Successful Response
* @throws ApiError
*/
export const ensureUseConfigServiceGetConfigData = (queryClient: QueryClient, { accept, section }: {
  accept?: "application/json" | "text/plain" | "*/*";
  section?: string;
} = {}) => queryClient.ensureQueryData({ queryKey: Common.UseConfigServiceGetConfigKeyFn({ accept, section }), queryFn: () => ConfigService.getConfig({ accept, section }) });
/**
* Get Config Value
* @param data The data for the request.
* @param data.section
* @param data.option
* @param data.accept
* @returns Config Successful Response
* @throws ApiError
*/
export const ensureUseConfigServiceGetConfigValueData = (queryClient: QueryClient, { accept, option, section }: {
  accept?: "application/json" | "text/plain" | "*/*";
  option: string;
  section: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseConfigServiceGetConfigValueKeyFn({ accept, option, section }), queryFn: () => ConfigService.getConfigValue({ accept, option, section }) });
/**
* Get Configs
* Get configs for UI.
* @returns ConfigResponse Successful Response
* @throws ApiError
*/
export const ensureUseConfigServiceGetConfigsData = (queryClient: QueryClient) => queryClient.ensureQueryData({ queryKey: Common.UseConfigServiceGetConfigsKeyFn(), queryFn: () => ConfigService.getConfigs() });
/**
* List Dag Warnings
* Get a list of DAG warnings.
* @param data The data for the request.
* @param data.dagId
* @param data.warningType
* @param data.limit
* @param data.offset
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `dag_id, warning_type, message, timestamp`
* @returns DAGWarningCollectionResponse Successful Response
* @throws ApiError
*/
export const ensureUseDagWarningServiceListDagWarningsData = (queryClient: QueryClient, { dagId, limit, offset, orderBy, warningType }: {
  dagId?: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
  warningType?: DagWarningType;
} = {}) => queryClient.ensureQueryData({ queryKey: Common.UseDagWarningServiceListDagWarningsKeyFn({ dagId, limit, offset, orderBy, warningType }), queryFn: () => DagWarningService.listDagWarnings({ dagId, limit, offset, orderBy, warningType }) });
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
* @param data.hasImportErrors Filter Dags by having import errors. Only Dags that have been successfully loaded before will be returned.
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
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `dag_id, dag_display_name, next_dagrun, state, start_date, last_run_state, last_run_start_date`
* @param data.isFavorite
* @returns DAGCollectionResponse Successful Response
* @throws ApiError
*/
export const ensureUseDagServiceGetDagsData = (queryClient: QueryClient, { assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagIdPattern, dagRunEndDateGt, dagRunEndDateGte, dagRunEndDateLt, dagRunEndDateLte, dagRunStartDateGt, dagRunStartDateGte, dagRunStartDateLt, dagRunStartDateLte, dagRunState, excludeStale, hasAssetSchedule, hasImportErrors, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode }: {
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
  hasImportErrors?: boolean;
  isFavorite?: boolean;
  lastDagRunState?: DagRunState;
  limit?: number;
  offset?: number;
  orderBy?: string[];
  owners?: string[];
  paused?: boolean;
  tags?: string[];
  tagsMatchMode?: "any" | "all";
} = {}) => queryClient.ensureQueryData({ queryKey: Common.UseDagServiceGetDagsKeyFn({ assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagIdPattern, dagRunEndDateGt, dagRunEndDateGte, dagRunEndDateLt, dagRunEndDateLte, dagRunStartDateGt, dagRunStartDateGte, dagRunStartDateLt, dagRunStartDateLte, dagRunState, excludeStale, hasAssetSchedule, hasImportErrors, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode }), queryFn: () => DagService.getDags({ assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagIdPattern, dagRunEndDateGt, dagRunEndDateGte, dagRunEndDateLt, dagRunEndDateLte, dagRunStartDateGt, dagRunStartDateGte, dagRunStartDateLt, dagRunStartDateLte, dagRunState, excludeStale, hasAssetSchedule, hasImportErrors, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode }) });
/**
* Get Dag
* Get basic information about a DAG.
* @param data The data for the request.
* @param data.dagId
* @returns DAGResponse Successful Response
* @throws ApiError
*/
export const ensureUseDagServiceGetDagData = (queryClient: QueryClient, { dagId }: {
  dagId: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseDagServiceGetDagKeyFn({ dagId }), queryFn: () => DagService.getDag({ dagId }) });
/**
* Get Dag Details
* Get details of DAG.
* @param data The data for the request.
* @param data.dagId
* @returns DAGDetailsResponse Successful Response
* @throws ApiError
*/
export const ensureUseDagServiceGetDagDetailsData = (queryClient: QueryClient, { dagId }: {
  dagId: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseDagServiceGetDagDetailsKeyFn({ dagId }), queryFn: () => DagService.getDagDetails({ dagId }) });
/**
* Get Dag Tags
* Get all DAG tags.
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `name`
* @param data.tagNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @returns DAGTagCollectionResponse Successful Response
* @throws ApiError
*/
export const ensureUseDagServiceGetDagTagsData = (queryClient: QueryClient, { limit, offset, orderBy, tagNamePattern }: {
  limit?: number;
  offset?: number;
  orderBy?: string[];
  tagNamePattern?: string;
} = {}) => queryClient.ensureQueryData({ queryKey: Common.UseDagServiceGetDagTagsKeyFn({ limit, offset, orderBy, tagNamePattern }), queryFn: () => DagService.getDagTags({ limit, offset, orderBy, tagNamePattern }) });
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
* @param data.hasImportErrors Filter Dags by having import errors. Only Dags that have been successfully loaded before will be returned.
* @param data.lastDagRunState
* @param data.bundleName
* @param data.bundleVersion
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `dag_id, dag_display_name, next_dagrun, state, start_date, last_run_state, last_run_start_date`
* @param data.isFavorite
* @param data.hasAssetSchedule Filter Dags with asset-based scheduling
* @param data.assetDependency Filter Dags by asset dependency (name or URI)
* @param data.hasPendingActions
* @returns DAGWithLatestDagRunsCollectionResponse Successful Response
* @throws ApiError
*/
export const ensureUseDagServiceGetDagsUiData = (queryClient: QueryClient, { assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagIdPattern, dagIds, dagRunsLimit, excludeStale, hasAssetSchedule, hasImportErrors, hasPendingActions, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode }: {
  assetDependency?: string;
  bundleName?: string;
  bundleVersion?: string;
  dagDisplayNamePattern?: string;
  dagIdPattern?: string;
  dagIds?: string[];
  dagRunsLimit?: number;
  excludeStale?: boolean;
  hasAssetSchedule?: boolean;
  hasImportErrors?: boolean;
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
} = {}) => queryClient.ensureQueryData({ queryKey: Common.UseDagServiceGetDagsUiKeyFn({ assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagIdPattern, dagIds, dagRunsLimit, excludeStale, hasAssetSchedule, hasImportErrors, hasPendingActions, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode }), queryFn: () => DagService.getDagsUi({ assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagIdPattern, dagIds, dagRunsLimit, excludeStale, hasAssetSchedule, hasImportErrors, hasPendingActions, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode }) });
/**
* Get Latest Run Info
* Get latest run.
* @param data The data for the request.
* @param data.dagId
* @returns unknown Successful Response
* @throws ApiError
*/
export const ensureUseDagServiceGetLatestRunInfoData = (queryClient: QueryClient, { dagId }: {
  dagId: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseDagServiceGetLatestRunInfoKeyFn({ dagId }), queryFn: () => DagService.getLatestRunInfo({ dagId }) });
/**
* Get Event Log
* @param data The data for the request.
* @param data.eventLogId
* @returns EventLogResponse Successful Response
* @throws ApiError
*/
export const ensureUseEventLogServiceGetEventLogData = (queryClient: QueryClient, { eventLogId }: {
  eventLogId: number;
}) => queryClient.ensureQueryData({ queryKey: Common.UseEventLogServiceGetEventLogKeyFn({ eventLogId }), queryFn: () => EventLogService.getEventLog({ eventLogId }) });
/**
* Get Event Logs
* Get all Event Logs.
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `id, dttm, dag_id, task_id, run_id, event, logical_date, owner, extra, when, event_log_id`
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
export const ensureUseEventLogServiceGetEventLogsData = (queryClient: QueryClient, { after, before, dagId, dagIdPattern, event, eventPattern, excludedEvents, includedEvents, limit, mapIndex, offset, orderBy, owner, ownerPattern, runId, runIdPattern, taskId, taskIdPattern, tryNumber }: {
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
} = {}) => queryClient.ensureQueryData({ queryKey: Common.UseEventLogServiceGetEventLogsKeyFn({ after, before, dagId, dagIdPattern, event, eventPattern, excludedEvents, includedEvents, limit, mapIndex, offset, orderBy, owner, ownerPattern, runId, runIdPattern, taskId, taskIdPattern, tryNumber }), queryFn: () => EventLogService.getEventLogs({ after, before, dagId, dagIdPattern, event, eventPattern, excludedEvents, includedEvents, limit, mapIndex, offset, orderBy, owner, ownerPattern, runId, runIdPattern, taskId, taskIdPattern, tryNumber }) });
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
export const ensureUseExtraLinksServiceGetExtraLinksData = (queryClient: QueryClient, { dagId, dagRunId, mapIndex, taskId }: {
  dagId: string;
  dagRunId: string;
  mapIndex?: number;
  taskId: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseExtraLinksServiceGetExtraLinksKeyFn({ dagId, dagRunId, mapIndex, taskId }), queryFn: () => ExtraLinksService.getExtraLinks({ dagId, dagRunId, mapIndex, taskId }) });
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
export const ensureUseTaskInstanceServiceGetExtraLinksData = (queryClient: QueryClient, { dagId, dagRunId, mapIndex, taskId }: {
  dagId: string;
  dagRunId: string;
  mapIndex?: number;
  taskId: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseTaskInstanceServiceGetExtraLinksKeyFn({ dagId, dagRunId, mapIndex, taskId }), queryFn: () => TaskInstanceService.getExtraLinks({ dagId, dagRunId, mapIndex, taskId }) });
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
export const ensureUseTaskInstanceServiceGetTaskInstanceData = (queryClient: QueryClient, { dagId, dagRunId, taskId }: {
  dagId: string;
  dagRunId: string;
  taskId: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseTaskInstanceServiceGetTaskInstanceKeyFn({ dagId, dagRunId, taskId }), queryFn: () => TaskInstanceService.getTaskInstance({ dagId, dagRunId, taskId }) });
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
* @param data.poolNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.queue
* @param data.queueNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.executor
* @param data.versionNumber
* @param data.tryNumber
* @param data.operator
* @param data.operatorNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.mapIndex
* @param data.limit
* @param data.offset
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `id, state, duration, start_date, end_date, map_index, try_number, logical_date, run_after, data_interval_start, data_interval_end, rendered_map_index, operator, run_after, logical_date, data_interval_start, data_interval_end`
* @returns TaskInstanceCollectionResponse Successful Response
* @throws ApiError
*/
export const ensureUseTaskInstanceServiceGetMappedTaskInstancesData = (queryClient: QueryClient, { dagId, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, offset, operator, operatorNamePattern, orderBy, pool, poolNamePattern, queue, queueNamePattern, runAfterGt, runAfterGte, runAfterLt, runAfterLte, startDateGt, startDateGte, startDateLt, startDateLte, state, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }: {
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
  mapIndex?: number[];
  offset?: number;
  operator?: string[];
  operatorNamePattern?: string;
  orderBy?: string[];
  pool?: string[];
  poolNamePattern?: string;
  queue?: string[];
  queueNamePattern?: string;
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
}) => queryClient.ensureQueryData({ queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstancesKeyFn({ dagId, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, offset, operator, operatorNamePattern, orderBy, pool, poolNamePattern, queue, queueNamePattern, runAfterGt, runAfterGte, runAfterLt, runAfterLte, startDateGt, startDateGte, startDateLt, startDateLte, state, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }), queryFn: () => TaskInstanceService.getMappedTaskInstances({ dagId, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, offset, operator, operatorNamePattern, orderBy, pool, poolNamePattern, queue, queueNamePattern, runAfterGt, runAfterGte, runAfterLt, runAfterLte, startDateGt, startDateGte, startDateLt, startDateLte, state, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }) });
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
export const ensureUseTaskInstanceServiceGetTaskInstanceDependenciesByMapIndexData = (queryClient: QueryClient, { dagId, dagRunId, mapIndex, taskId }: {
  dagId: string;
  dagRunId: string;
  mapIndex: number;
  taskId: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseTaskInstanceServiceGetTaskInstanceDependenciesByMapIndexKeyFn({ dagId, dagRunId, mapIndex, taskId }), queryFn: () => TaskInstanceService.getTaskInstanceDependenciesByMapIndex({ dagId, dagRunId, mapIndex, taskId }) });
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
export const ensureUseTaskInstanceServiceGetTaskInstanceDependenciesData = (queryClient: QueryClient, { dagId, dagRunId, mapIndex, taskId }: {
  dagId: string;
  dagRunId: string;
  mapIndex?: number;
  taskId: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseTaskInstanceServiceGetTaskInstanceDependenciesKeyFn({ dagId, dagRunId, mapIndex, taskId }), queryFn: () => TaskInstanceService.getTaskInstanceDependencies({ dagId, dagRunId, mapIndex, taskId }) });
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
export const ensureUseTaskInstanceServiceGetTaskInstanceTriesData = (queryClient: QueryClient, { dagId, dagRunId, mapIndex, taskId }: {
  dagId: string;
  dagRunId: string;
  mapIndex?: number;
  taskId: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseTaskInstanceServiceGetTaskInstanceTriesKeyFn({ dagId, dagRunId, mapIndex, taskId }), queryFn: () => TaskInstanceService.getTaskInstanceTries({ dagId, dagRunId, mapIndex, taskId }) });
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
export const ensureUseTaskInstanceServiceGetMappedTaskInstanceTriesData = (queryClient: QueryClient, { dagId, dagRunId, mapIndex, taskId }: {
  dagId: string;
  dagRunId: string;
  mapIndex: number;
  taskId: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstanceTriesKeyFn({ dagId, dagRunId, mapIndex, taskId }), queryFn: () => TaskInstanceService.getMappedTaskInstanceTries({ dagId, dagRunId, mapIndex, taskId }) });
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
export const ensureUseTaskInstanceServiceGetMappedTaskInstanceData = (queryClient: QueryClient, { dagId, dagRunId, mapIndex, taskId }: {
  dagId: string;
  dagRunId: string;
  mapIndex: number;
  taskId: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstanceKeyFn({ dagId, dagRunId, mapIndex, taskId }), queryFn: () => TaskInstanceService.getMappedTaskInstance({ dagId, dagRunId, mapIndex, taskId }) });
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
* @param data.taskGroupId Filter by exact task group ID. Returns all tasks within the specified task group.
* @param data.dagIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.runIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.state
* @param data.pool
* @param data.poolNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.queue
* @param data.queueNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.executor
* @param data.versionNumber
* @param data.tryNumber
* @param data.operator
* @param data.operatorNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.mapIndex
* @param data.limit
* @param data.offset
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `id, state, duration, start_date, end_date, map_index, try_number, logical_date, run_after, data_interval_start, data_interval_end, rendered_map_index, operator, logical_date, run_after, data_interval_start, data_interval_end`
* @returns TaskInstanceCollectionResponse Successful Response
* @throws ApiError
*/
export const ensureUseTaskInstanceServiceGetTaskInstancesData = (queryClient: QueryClient, { dagId, dagIdPattern, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, offset, operator, operatorNamePattern, orderBy, pool, poolNamePattern, queue, queueNamePattern, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, startDateGt, startDateGte, startDateLt, startDateLte, state, taskDisplayNamePattern, taskGroupId, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }: {
  dagId: string;
  dagIdPattern?: string;
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
  mapIndex?: number[];
  offset?: number;
  operator?: string[];
  operatorNamePattern?: string;
  orderBy?: string[];
  pool?: string[];
  poolNamePattern?: string;
  queue?: string[];
  queueNamePattern?: string;
  runAfterGt?: string;
  runAfterGte?: string;
  runAfterLt?: string;
  runAfterLte?: string;
  runIdPattern?: string;
  startDateGt?: string;
  startDateGte?: string;
  startDateLt?: string;
  startDateLte?: string;
  state?: string[];
  taskDisplayNamePattern?: string;
  taskGroupId?: string;
  taskId?: string;
  tryNumber?: number[];
  updatedAtGt?: string;
  updatedAtGte?: string;
  updatedAtLt?: string;
  updatedAtLte?: string;
  versionNumber?: number[];
}) => queryClient.ensureQueryData({ queryKey: Common.UseTaskInstanceServiceGetTaskInstancesKeyFn({ dagId, dagIdPattern, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, offset, operator, operatorNamePattern, orderBy, pool, poolNamePattern, queue, queueNamePattern, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, startDateGt, startDateGte, startDateLt, startDateLte, state, taskDisplayNamePattern, taskGroupId, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }), queryFn: () => TaskInstanceService.getTaskInstances({ dagId, dagIdPattern, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, offset, operator, operatorNamePattern, orderBy, pool, poolNamePattern, queue, queueNamePattern, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, startDateGt, startDateGte, startDateLt, startDateLte, state, taskDisplayNamePattern, taskGroupId, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }) });
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
export const ensureUseTaskInstanceServiceGetTaskInstanceTryDetailsData = (queryClient: QueryClient, { dagId, dagRunId, mapIndex, taskId, taskTryNumber }: {
  dagId: string;
  dagRunId: string;
  mapIndex?: number;
  taskId: string;
  taskTryNumber: number;
}) => queryClient.ensureQueryData({ queryKey: Common.UseTaskInstanceServiceGetTaskInstanceTryDetailsKeyFn({ dagId, dagRunId, mapIndex, taskId, taskTryNumber }), queryFn: () => TaskInstanceService.getTaskInstanceTryDetails({ dagId, dagRunId, mapIndex, taskId, taskTryNumber }) });
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
export const ensureUseTaskInstanceServiceGetMappedTaskInstanceTryDetailsData = (queryClient: QueryClient, { dagId, dagRunId, mapIndex, taskId, taskTryNumber }: {
  dagId: string;
  dagRunId: string;
  mapIndex: number;
  taskId: string;
  taskTryNumber: number;
}) => queryClient.ensureQueryData({ queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstanceTryDetailsKeyFn({ dagId, dagRunId, mapIndex, taskId, taskTryNumber }), queryFn: () => TaskInstanceService.getMappedTaskInstanceTryDetails({ dagId, dagRunId, mapIndex, taskId, taskTryNumber }) });
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
export const ensureUseTaskInstanceServiceGetLogData = (queryClient: QueryClient, { accept, dagId, dagRunId, fullContent, mapIndex, taskId, token, tryNumber }: {
  accept?: "application/json" | "*/*" | "application/x-ndjson";
  dagId: string;
  dagRunId: string;
  fullContent?: boolean;
  mapIndex?: number;
  taskId: string;
  token?: string;
  tryNumber: number;
}) => queryClient.ensureQueryData({ queryKey: Common.UseTaskInstanceServiceGetLogKeyFn({ accept, dagId, dagRunId, fullContent, mapIndex, taskId, token, tryNumber }), queryFn: () => TaskInstanceService.getLog({ accept, dagId, dagRunId, fullContent, mapIndex, taskId, token, tryNumber }) });
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
export const ensureUseTaskInstanceServiceGetExternalLogUrlData = (queryClient: QueryClient, { dagId, dagRunId, mapIndex, taskId, tryNumber }: {
  dagId: string;
  dagRunId: string;
  mapIndex?: number;
  taskId: string;
  tryNumber: number;
}) => queryClient.ensureQueryData({ queryKey: Common.UseTaskInstanceServiceGetExternalLogUrlKeyFn({ dagId, dagRunId, mapIndex, taskId, tryNumber }), queryFn: () => TaskInstanceService.getExternalLogUrl({ dagId, dagRunId, mapIndex, taskId, tryNumber }) });
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
export const ensureUseTaskInstanceServiceGetHitlDetailData = (queryClient: QueryClient, { dagId, dagRunId, mapIndex, taskId }: {
  dagId: string;
  dagRunId: string;
  mapIndex: number;
  taskId: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseTaskInstanceServiceGetHitlDetailKeyFn({ dagId, dagRunId, mapIndex, taskId }), queryFn: () => TaskInstanceService.getHitlDetail({ dagId, dagRunId, mapIndex, taskId }) });
/**
* Get Hitl Details
* Get Human-in-the-loop details.
* @param data The data for the request.
* @param data.dagId
* @param data.dagRunId
* @param data.limit
* @param data.offset
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `ti_id, subject, responded_at, created_at, responded_by_user_id, responded_by_user_name, dag_id, run_id, task_display_name, run_after, rendered_map_index, task_instance_operator, task_instance_state`
* @param data.dagIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.taskId
* @param data.taskIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.mapIndex
* @param data.state
* @param data.responseReceived
* @param data.respondedByUserId
* @param data.respondedByUserName
* @param data.subjectSearch SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.bodySearch SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.createdAtGte
* @param data.createdAtGt
* @param data.createdAtLte
* @param data.createdAtLt
* @returns HITLDetailCollection Successful Response
* @throws ApiError
*/
export const ensureUseTaskInstanceServiceGetHitlDetailsData = (queryClient: QueryClient, { bodySearch, createdAtGt, createdAtGte, createdAtLt, createdAtLte, dagId, dagIdPattern, dagRunId, limit, mapIndex, offset, orderBy, respondedByUserId, respondedByUserName, responseReceived, state, subjectSearch, taskId, taskIdPattern }: {
  bodySearch?: string;
  createdAtGt?: string;
  createdAtGte?: string;
  createdAtLt?: string;
  createdAtLte?: string;
  dagId: string;
  dagIdPattern?: string;
  dagRunId: string;
  limit?: number;
  mapIndex?: number;
  offset?: number;
  orderBy?: string[];
  respondedByUserId?: string[];
  respondedByUserName?: string[];
  responseReceived?: boolean;
  state?: string[];
  subjectSearch?: string;
  taskId?: string;
  taskIdPattern?: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseTaskInstanceServiceGetHitlDetailsKeyFn({ bodySearch, createdAtGt, createdAtGte, createdAtLt, createdAtLte, dagId, dagIdPattern, dagRunId, limit, mapIndex, offset, orderBy, respondedByUserId, respondedByUserName, responseReceived, state, subjectSearch, taskId, taskIdPattern }), queryFn: () => TaskInstanceService.getHitlDetails({ bodySearch, createdAtGt, createdAtGte, createdAtLt, createdAtLte, dagId, dagIdPattern, dagRunId, limit, mapIndex, offset, orderBy, respondedByUserId, respondedByUserName, responseReceived, state, subjectSearch, taskId, taskIdPattern }) });
/**
* Get Import Error
* Get an import error.
* @param data The data for the request.
* @param data.importErrorId
* @returns ImportErrorResponse Successful Response
* @throws ApiError
*/
export const ensureUseImportErrorServiceGetImportErrorData = (queryClient: QueryClient, { importErrorId }: {
  importErrorId: number;
}) => queryClient.ensureQueryData({ queryKey: Common.UseImportErrorServiceGetImportErrorKeyFn({ importErrorId }), queryFn: () => ImportErrorService.getImportError({ importErrorId }) });
/**
* Get Import Errors
* Get all import errors.
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `id, timestamp, filename, bundle_name, stacktrace, import_error_id`
* @param data.filenamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @returns ImportErrorCollectionResponse Successful Response
* @throws ApiError
*/
export const ensureUseImportErrorServiceGetImportErrorsData = (queryClient: QueryClient, { filenamePattern, limit, offset, orderBy }: {
  filenamePattern?: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
} = {}) => queryClient.ensureQueryData({ queryKey: Common.UseImportErrorServiceGetImportErrorsKeyFn({ filenamePattern, limit, offset, orderBy }), queryFn: () => ImportErrorService.getImportErrors({ filenamePattern, limit, offset, orderBy }) });
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
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `id, dag_id, state, job_type, start_date, end_date, latest_heartbeat, executor_class, hostname, unixname`
* @param data.jobState
* @param data.jobType
* @param data.hostname
* @param data.executorClass
* @returns JobCollectionResponse Successful Response
* @throws ApiError
*/
export const ensureUseJobServiceGetJobsData = (queryClient: QueryClient, { endDateGt, endDateGte, endDateLt, endDateLte, executorClass, hostname, isAlive, jobState, jobType, limit, offset, orderBy, startDateGt, startDateGte, startDateLt, startDateLte }: {
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
} = {}) => queryClient.ensureQueryData({ queryKey: Common.UseJobServiceGetJobsKeyFn({ endDateGt, endDateGte, endDateLt, endDateLte, executorClass, hostname, isAlive, jobState, jobType, limit, offset, orderBy, startDateGt, startDateGte, startDateLt, startDateLte }), queryFn: () => JobService.getJobs({ endDateGt, endDateGte, endDateLt, endDateLte, executorClass, hostname, isAlive, jobState, jobType, limit, offset, orderBy, startDateGt, startDateGte, startDateLt, startDateLte }) });
/**
* Get Plugins
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @returns PluginCollectionResponse Successful Response
* @throws ApiError
*/
export const ensureUsePluginServiceGetPluginsData = (queryClient: QueryClient, { limit, offset }: {
  limit?: number;
  offset?: number;
} = {}) => queryClient.ensureQueryData({ queryKey: Common.UsePluginServiceGetPluginsKeyFn({ limit, offset }), queryFn: () => PluginService.getPlugins({ limit, offset }) });
/**
* Import Errors
* @returns PluginImportErrorCollectionResponse Successful Response
* @throws ApiError
*/
export const ensureUsePluginServiceImportErrorsData = (queryClient: QueryClient) => queryClient.ensureQueryData({ queryKey: Common.UsePluginServiceImportErrorsKeyFn(), queryFn: () => PluginService.importErrors() });
/**
* Get Pool
* Get a pool.
* @param data The data for the request.
* @param data.poolName
* @returns PoolResponse Successful Response
* @throws ApiError
*/
export const ensureUsePoolServiceGetPoolData = (queryClient: QueryClient, { poolName }: {
  poolName: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UsePoolServiceGetPoolKeyFn({ poolName }), queryFn: () => PoolService.getPool({ poolName }) });
/**
* Get Pools
* Get all pools entries.
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `id, pool, name`
* @param data.poolNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @returns PoolCollectionResponse Successful Response
* @throws ApiError
*/
export const ensureUsePoolServiceGetPoolsData = (queryClient: QueryClient, { limit, offset, orderBy, poolNamePattern }: {
  limit?: number;
  offset?: number;
  orderBy?: string[];
  poolNamePattern?: string;
} = {}) => queryClient.ensureQueryData({ queryKey: Common.UsePoolServiceGetPoolsKeyFn({ limit, offset, orderBy, poolNamePattern }), queryFn: () => PoolService.getPools({ limit, offset, orderBy, poolNamePattern }) });
/**
* Get Providers
* Get providers.
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @returns ProviderCollectionResponse Successful Response
* @throws ApiError
*/
export const ensureUseProviderServiceGetProvidersData = (queryClient: QueryClient, { limit, offset }: {
  limit?: number;
  offset?: number;
} = {}) => queryClient.ensureQueryData({ queryKey: Common.UseProviderServiceGetProvidersKeyFn({ limit, offset }), queryFn: () => ProviderService.getProviders({ limit, offset }) });
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
export const ensureUseXcomServiceGetXcomEntryData = (queryClient: QueryClient, { dagId, dagRunId, deserialize, mapIndex, stringify, taskId, xcomKey }: {
  dagId: string;
  dagRunId: string;
  deserialize?: boolean;
  mapIndex?: number;
  stringify?: boolean;
  taskId: string;
  xcomKey: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseXcomServiceGetXcomEntryKeyFn({ dagId, dagRunId, deserialize, mapIndex, stringify, taskId, xcomKey }), queryFn: () => XcomService.getXcomEntry({ dagId, dagRunId, deserialize, mapIndex, stringify, taskId, xcomKey }) });
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
export const ensureUseXcomServiceGetXcomEntriesData = (queryClient: QueryClient, { dagDisplayNamePattern, dagId, dagRunId, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, mapIndexFilter, offset, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, taskId, taskIdPattern, xcomKey, xcomKeyPattern }: {
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
}) => queryClient.ensureQueryData({ queryKey: Common.UseXcomServiceGetXcomEntriesKeyFn({ dagDisplayNamePattern, dagId, dagRunId, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, mapIndexFilter, offset, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, taskId, taskIdPattern, xcomKey, xcomKeyPattern }), queryFn: () => XcomService.getXcomEntries({ dagDisplayNamePattern, dagId, dagRunId, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, mapIndexFilter, offset, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, taskId, taskIdPattern, xcomKey, xcomKeyPattern }) });
/**
* Get Tasks
* Get tasks for DAG.
* @param data The data for the request.
* @param data.dagId
* @param data.orderBy
* @returns TaskCollectionResponse Successful Response
* @throws ApiError
*/
export const ensureUseTaskServiceGetTasksData = (queryClient: QueryClient, { dagId, orderBy }: {
  dagId: string;
  orderBy?: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseTaskServiceGetTasksKeyFn({ dagId, orderBy }), queryFn: () => TaskService.getTasks({ dagId, orderBy }) });
/**
* Get Task
* Get simplified representation of a task.
* @param data The data for the request.
* @param data.dagId
* @param data.taskId
* @returns TaskResponse Successful Response
* @throws ApiError
*/
export const ensureUseTaskServiceGetTaskData = (queryClient: QueryClient, { dagId, taskId }: {
  dagId: string;
  taskId: unknown;
}) => queryClient.ensureQueryData({ queryKey: Common.UseTaskServiceGetTaskKeyFn({ dagId, taskId }), queryFn: () => TaskService.getTask({ dagId, taskId }) });
/**
* Get Variable
* Get a variable entry.
* @param data The data for the request.
* @param data.variableKey
* @returns VariableResponse Successful Response
* @throws ApiError
*/
export const ensureUseVariableServiceGetVariableData = (queryClient: QueryClient, { variableKey }: {
  variableKey: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseVariableServiceGetVariableKeyFn({ variableKey }), queryFn: () => VariableService.getVariable({ variableKey }) });
/**
* Get Variables
* Get all Variables entries.
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `key, id, _val, description, is_encrypted`
* @param data.variableKeyPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @returns VariableCollectionResponse Successful Response
* @throws ApiError
*/
export const ensureUseVariableServiceGetVariablesData = (queryClient: QueryClient, { limit, offset, orderBy, variableKeyPattern }: {
  limit?: number;
  offset?: number;
  orderBy?: string[];
  variableKeyPattern?: string;
} = {}) => queryClient.ensureQueryData({ queryKey: Common.UseVariableServiceGetVariablesKeyFn({ limit, offset, orderBy, variableKeyPattern }), queryFn: () => VariableService.getVariables({ limit, offset, orderBy, variableKeyPattern }) });
/**
* Get Dag Version
* Get one Dag Version.
* @param data The data for the request.
* @param data.dagId
* @param data.versionNumber
* @returns DagVersionResponse Successful Response
* @throws ApiError
*/
export const ensureUseDagVersionServiceGetDagVersionData = (queryClient: QueryClient, { dagId, versionNumber }: {
  dagId: string;
  versionNumber: number;
}) => queryClient.ensureQueryData({ queryKey: Common.UseDagVersionServiceGetDagVersionKeyFn({ dagId, versionNumber }), queryFn: () => DagVersionService.getDagVersion({ dagId, versionNumber }) });
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
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `id, version_number, bundle_name, bundle_version`
* @returns DAGVersionCollectionResponse Successful Response
* @throws ApiError
*/
export const ensureUseDagVersionServiceGetDagVersionsData = (queryClient: QueryClient, { bundleName, bundleVersion, dagId, limit, offset, orderBy, versionNumber }: {
  bundleName?: string;
  bundleVersion?: string;
  dagId: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
  versionNumber?: number;
}) => queryClient.ensureQueryData({ queryKey: Common.UseDagVersionServiceGetDagVersionsKeyFn({ bundleName, bundleVersion, dagId, limit, offset, orderBy, versionNumber }), queryFn: () => DagVersionService.getDagVersions({ bundleName, bundleVersion, dagId, limit, offset, orderBy, versionNumber }) });
/**
* Get Health
* @returns HealthInfoResponse Successful Response
* @throws ApiError
*/
export const ensureUseMonitorServiceGetHealthData = (queryClient: QueryClient) => queryClient.ensureQueryData({ queryKey: Common.UseMonitorServiceGetHealthKeyFn(), queryFn: () => MonitorService.getHealth() });
/**
* Get Version
* Get version information.
* @returns VersionInfo Successful Response
* @throws ApiError
*/
export const ensureUseVersionServiceGetVersionData = (queryClient: QueryClient) => queryClient.ensureQueryData({ queryKey: Common.UseVersionServiceGetVersionKeyFn(), queryFn: () => VersionService.getVersion() });
/**
* Login
* Redirect to the login URL depending on the AuthManager configured.
* @param data The data for the request.
* @param data.next
* @returns unknown Successful Response
* @throws ApiError
*/
export const ensureUseLoginServiceLoginData = (queryClient: QueryClient, { next }: {
  next?: string;
} = {}) => queryClient.ensureQueryData({ queryKey: Common.UseLoginServiceLoginKeyFn({ next }), queryFn: () => LoginService.login({ next }) });
/**
* Logout
* Logout the user.
* @returns unknown Successful Response
* @throws ApiError
*/
export const ensureUseLoginServiceLogoutData = (queryClient: QueryClient) => queryClient.ensureQueryData({ queryKey: Common.UseLoginServiceLogoutKeyFn(), queryFn: () => LoginService.logout() });
/**
* Get Auth Menus
* @returns MenuItemCollectionResponse Successful Response
* @throws ApiError
*/
export const ensureUseAuthLinksServiceGetAuthMenusData = (queryClient: QueryClient) => queryClient.ensureQueryData({ queryKey: Common.UseAuthLinksServiceGetAuthMenusKeyFn(), queryFn: () => AuthLinksService.getAuthMenus() });
/**
* Get Current User Info
* Convienently get the current authenticated user information.
* @returns AuthenticatedMeResponse Successful Response
* @throws ApiError
*/
export const ensureUseAuthLinksServiceGetCurrentUserInfoData = (queryClient: QueryClient) => queryClient.ensureQueryData({ queryKey: Common.UseAuthLinksServiceGetCurrentUserInfoKeyFn(), queryFn: () => AuthLinksService.getCurrentUserInfo() });
/**
* Get Dependencies
* Dependencies graph.
* @param data The data for the request.
* @param data.nodeId
* @returns BaseGraphResponse Successful Response
* @throws ApiError
*/
export const ensureUseDependenciesServiceGetDependenciesData = (queryClient: QueryClient, { nodeId }: {
  nodeId?: string;
} = {}) => queryClient.ensureQueryData({ queryKey: Common.UseDependenciesServiceGetDependenciesKeyFn({ nodeId }), queryFn: () => DependenciesService.getDependencies({ nodeId }) });
/**
* Historical Metrics
* Return cluster activity historical metrics.
* @param data The data for the request.
* @param data.startDate
* @param data.endDate
* @returns HistoricalMetricDataResponse Successful Response
* @throws ApiError
*/
export const ensureUseDashboardServiceHistoricalMetricsData = (queryClient: QueryClient, { endDate, startDate }: {
  endDate?: string;
  startDate: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseDashboardServiceHistoricalMetricsKeyFn({ endDate, startDate }), queryFn: () => DashboardService.historicalMetrics({ endDate, startDate }) });
/**
* Dag Stats
* Return basic DAG stats with counts of DAGs in various states.
* @returns DashboardDagStatsResponse Successful Response
* @throws ApiError
*/
export const ensureUseDashboardServiceDagStatsData = (queryClient: QueryClient) => queryClient.ensureQueryData({ queryKey: Common.UseDashboardServiceDagStatsKeyFn(), queryFn: () => DashboardService.dagStats() });
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
export const ensureUseStructureServiceStructureDataData = (queryClient: QueryClient, { dagId, externalDependencies, includeDownstream, includeUpstream, root, versionNumber }: {
  dagId: string;
  externalDependencies?: boolean;
  includeDownstream?: boolean;
  includeUpstream?: boolean;
  root?: string;
  versionNumber?: number;
}) => queryClient.ensureQueryData({ queryKey: Common.UseStructureServiceStructureDataKeyFn({ dagId, externalDependencies, includeDownstream, includeUpstream, root, versionNumber }), queryFn: () => StructureService.structureData({ dagId, externalDependencies, includeDownstream, includeUpstream, root, versionNumber }) });
/**
* Get Dag Structure
* Return dag structure for grid view.
* @param data The data for the request.
* @param data.dagId
* @param data.offset
* @param data.limit
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `run_after, logical_date, start_date, end_date`
* @param data.runAfterGte
* @param data.runAfterGt
* @param data.runAfterLte
* @param data.runAfterLt
* @param data.runType
* @param data.state
* @param data.triggeringUser SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @returns GridNodeResponse Successful Response
* @throws ApiError
*/
export const ensureUseGridServiceGetDagStructureData = (queryClient: QueryClient, { dagId, limit, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runType, state, triggeringUser }: {
  dagId: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
  runAfterGt?: string;
  runAfterGte?: string;
  runAfterLt?: string;
  runAfterLte?: string;
  runType?: string[];
  state?: string[];
  triggeringUser?: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseGridServiceGetDagStructureKeyFn({ dagId, limit, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runType, state, triggeringUser }), queryFn: () => GridService.getDagStructure({ dagId, limit, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runType, state, triggeringUser }) });
/**
* Get Grid Runs
* Get info about a run for the grid.
* @param data The data for the request.
* @param data.dagId
* @param data.offset
* @param data.limit
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `run_after, logical_date, start_date, end_date`
* @param data.runAfterGte
* @param data.runAfterGt
* @param data.runAfterLte
* @param data.runAfterLt
* @param data.runType
* @param data.state
* @param data.triggeringUser SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @returns GridRunsResponse Successful Response
* @throws ApiError
*/
export const ensureUseGridServiceGetGridRunsData = (queryClient: QueryClient, { dagId, limit, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runType, state, triggeringUser }: {
  dagId: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
  runAfterGt?: string;
  runAfterGte?: string;
  runAfterLt?: string;
  runAfterLte?: string;
  runType?: string[];
  state?: string[];
  triggeringUser?: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseGridServiceGetGridRunsKeyFn({ dagId, limit, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runType, state, triggeringUser }), queryFn: () => GridService.getGridRuns({ dagId, limit, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runType, state, triggeringUser }) });
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
export const ensureUseGridServiceGetGridTiSummariesData = (queryClient: QueryClient, { dagId, runId }: {
  dagId: string;
  runId: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseGridServiceGetGridTiSummariesKeyFn({ dagId, runId }), queryFn: () => GridService.getGridTiSummaries({ dagId, runId }) });
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
export const ensureUseCalendarServiceGetCalendarData = (queryClient: QueryClient, { dagId, granularity, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte }: {
  dagId: string;
  granularity?: "hourly" | "daily";
  logicalDateGt?: string;
  logicalDateGte?: string;
  logicalDateLt?: string;
  logicalDateLte?: string;
}) => queryClient.ensureQueryData({ queryKey: Common.UseCalendarServiceGetCalendarKeyFn({ dagId, granularity, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte }), queryFn: () => CalendarService.getCalendar({ dagId, granularity, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte }) });
/**
* List Teams
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `name`
* @returns TeamCollectionResponse Successful Response
* @throws ApiError
*/
export const ensureUseTeamsServiceListTeamsData = (queryClient: QueryClient, { limit, offset, orderBy }: {
  limit?: number;
  offset?: number;
  orderBy?: string[];
} = {}) => queryClient.ensureQueryData({ queryKey: Common.UseTeamsServiceListTeamsKeyFn({ limit, offset, orderBy }), queryFn: () => TeamsService.listTeams({ limit, offset, orderBy }) });
