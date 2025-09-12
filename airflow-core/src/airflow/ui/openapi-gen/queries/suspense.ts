// generated with @7nohe/openapi-react-query-codegen@1.6.2 

import { UseQueryOptions, useSuspenseQuery } from "@tanstack/react-query";
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
export const useAssetServiceGetAssetsSuspense = <TData = Common.AssetServiceGetAssetsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagIds, limit, namePattern, offset, onlyActive, orderBy, uriPattern }: {
  dagIds?: string[];
  limit?: number;
  namePattern?: string;
  offset?: number;
  onlyActive?: boolean;
  orderBy?: string[];
  uriPattern?: string;
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseAssetServiceGetAssetsKeyFn({ dagIds, limit, namePattern, offset, onlyActive, orderBy, uriPattern }, queryKey), queryFn: () => AssetService.getAssets({ dagIds, limit, namePattern, offset, onlyActive, orderBy, uriPattern }) as TData, ...options });
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
export const useAssetServiceGetAssetAliasesSuspense = <TData = Common.AssetServiceGetAssetAliasesDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ limit, namePattern, offset, orderBy }: {
  limit?: number;
  namePattern?: string;
  offset?: number;
  orderBy?: string[];
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseAssetServiceGetAssetAliasesKeyFn({ limit, namePattern, offset, orderBy }, queryKey), queryFn: () => AssetService.getAssetAliases({ limit, namePattern, offset, orderBy }) as TData, ...options });
/**
* Get Asset Alias
* Get an asset alias.
* @param data The data for the request.
* @param data.assetAliasId
* @returns unknown Successful Response
* @throws ApiError
*/
export const useAssetServiceGetAssetAliasSuspense = <TData = Common.AssetServiceGetAssetAliasDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ assetAliasId }: {
  assetAliasId: number;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseAssetServiceGetAssetAliasKeyFn({ assetAliasId }, queryKey), queryFn: () => AssetService.getAssetAlias({ assetAliasId }) as TData, ...options });
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
export const useAssetServiceGetAssetEventsSuspense = <TData = Common.AssetServiceGetAssetEventsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ assetId, limit, offset, orderBy, sourceDagId, sourceMapIndex, sourceRunId, sourceTaskId, timestampGt, timestampGte, timestampLt, timestampLte }: {
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
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseAssetServiceGetAssetEventsKeyFn({ assetId, limit, offset, orderBy, sourceDagId, sourceMapIndex, sourceRunId, sourceTaskId, timestampGt, timestampGte, timestampLt, timestampLte }, queryKey), queryFn: () => AssetService.getAssetEvents({ assetId, limit, offset, orderBy, sourceDagId, sourceMapIndex, sourceRunId, sourceTaskId, timestampGt, timestampGte, timestampLt, timestampLte }) as TData, ...options });
/**
* Get Asset Queued Events
* Get queued asset events for an asset.
* @param data The data for the request.
* @param data.assetId
* @param data.before
* @returns QueuedEventCollectionResponse Successful Response
* @throws ApiError
*/
export const useAssetServiceGetAssetQueuedEventsSuspense = <TData = Common.AssetServiceGetAssetQueuedEventsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ assetId, before }: {
  assetId: number;
  before?: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseAssetServiceGetAssetQueuedEventsKeyFn({ assetId, before }, queryKey), queryFn: () => AssetService.getAssetQueuedEvents({ assetId, before }) as TData, ...options });
/**
* Get Asset
* Get an asset.
* @param data The data for the request.
* @param data.assetId
* @returns AssetResponse Successful Response
* @throws ApiError
*/
export const useAssetServiceGetAssetSuspense = <TData = Common.AssetServiceGetAssetDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ assetId }: {
  assetId: number;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseAssetServiceGetAssetKeyFn({ assetId }, queryKey), queryFn: () => AssetService.getAsset({ assetId }) as TData, ...options });
/**
* Get Dag Asset Queued Events
* Get queued asset events for a DAG.
* @param data The data for the request.
* @param data.dagId
* @param data.before
* @returns QueuedEventCollectionResponse Successful Response
* @throws ApiError
*/
export const useAssetServiceGetDagAssetQueuedEventsSuspense = <TData = Common.AssetServiceGetDagAssetQueuedEventsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ before, dagId }: {
  before?: string;
  dagId: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseAssetServiceGetDagAssetQueuedEventsKeyFn({ before, dagId }, queryKey), queryFn: () => AssetService.getDagAssetQueuedEvents({ before, dagId }) as TData, ...options });
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
export const useAssetServiceGetDagAssetQueuedEventSuspense = <TData = Common.AssetServiceGetDagAssetQueuedEventDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ assetId, before, dagId }: {
  assetId: number;
  before?: string;
  dagId: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseAssetServiceGetDagAssetQueuedEventKeyFn({ assetId, before, dagId }, queryKey), queryFn: () => AssetService.getDagAssetQueuedEvent({ assetId, before, dagId }) as TData, ...options });
/**
* Next Run Assets
* @param data The data for the request.
* @param data.dagId
* @returns unknown Successful Response
* @throws ApiError
*/
export const useAssetServiceNextRunAssetsSuspense = <TData = Common.AssetServiceNextRunAssetsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId }: {
  dagId: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseAssetServiceNextRunAssetsKeyFn({ dagId }, queryKey), queryFn: () => AssetService.nextRunAssets({ dagId }) as TData, ...options });
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
export const useBackfillServiceListBackfillsSuspense = <TData = Common.BackfillServiceListBackfillsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, limit, offset, orderBy }: {
  dagId: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseBackfillServiceListBackfillsKeyFn({ dagId, limit, offset, orderBy }, queryKey), queryFn: () => BackfillService.listBackfills({ dagId, limit, offset, orderBy }) as TData, ...options });
/**
* Get Backfill
* @param data The data for the request.
* @param data.backfillId
* @returns BackfillResponse Successful Response
* @throws ApiError
*/
export const useBackfillServiceGetBackfillSuspense = <TData = Common.BackfillServiceGetBackfillDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ backfillId }: {
  backfillId: number;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseBackfillServiceGetBackfillKeyFn({ backfillId }, queryKey), queryFn: () => BackfillService.getBackfill({ backfillId }) as TData, ...options });
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
export const useBackfillServiceListBackfillsUiSuspense = <TData = Common.BackfillServiceListBackfillsUiDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ active, dagId, limit, offset, orderBy }: {
  active?: boolean;
  dagId?: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseBackfillServiceListBackfillsUiKeyFn({ active, dagId, limit, offset, orderBy }, queryKey), queryFn: () => BackfillService.listBackfillsUi({ active, dagId, limit, offset, orderBy }) as TData, ...options });
/**
* Get Connection
* Get a connection entry.
* @param data The data for the request.
* @param data.connectionId
* @returns ConnectionResponse Successful Response
* @throws ApiError
*/
export const useConnectionServiceGetConnectionSuspense = <TData = Common.ConnectionServiceGetConnectionDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ connectionId }: {
  connectionId: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseConnectionServiceGetConnectionKeyFn({ connectionId }, queryKey), queryFn: () => ConnectionService.getConnection({ connectionId }) as TData, ...options });
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
export const useConnectionServiceGetConnectionsSuspense = <TData = Common.ConnectionServiceGetConnectionsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ connectionIdPattern, limit, offset, orderBy }: {
  connectionIdPattern?: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseConnectionServiceGetConnectionsKeyFn({ connectionIdPattern, limit, offset, orderBy }, queryKey), queryFn: () => ConnectionService.getConnections({ connectionIdPattern, limit, offset, orderBy }) as TData, ...options });
/**
* Hook Meta Data
* Retrieve information about available connection types (hook classes) and their parameters.
* @returns ConnectionHookMetaData Successful Response
* @throws ApiError
*/
export const useConnectionServiceHookMetaDataSuspense = <TData = Common.ConnectionServiceHookMetaDataDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>(queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseConnectionServiceHookMetaDataKeyFn(queryKey), queryFn: () => ConnectionService.hookMetaData() as TData, ...options });
/**
* Get Dag Run
* @param data The data for the request.
* @param data.dagId
* @param data.dagRunId
* @returns DAGRunResponse Successful Response
* @throws ApiError
*/
export const useDagRunServiceGetDagRunSuspense = <TData = Common.DagRunServiceGetDagRunDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, dagRunId }: {
  dagId: string;
  dagRunId: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseDagRunServiceGetDagRunKeyFn({ dagId, dagRunId }, queryKey), queryFn: () => DagRunService.getDagRun({ dagId, dagRunId }) as TData, ...options });
/**
* Get Upstream Asset Events
* If dag run is asset-triggered, return the asset events that triggered it.
* @param data The data for the request.
* @param data.dagId
* @param data.dagRunId
* @returns AssetEventCollectionResponse Successful Response
* @throws ApiError
*/
export const useDagRunServiceGetUpstreamAssetEventsSuspense = <TData = Common.DagRunServiceGetUpstreamAssetEventsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, dagRunId }: {
  dagId: string;
  dagRunId: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseDagRunServiceGetUpstreamAssetEventsKeyFn({ dagId, dagRunId }, queryKey), queryFn: () => DagRunService.getUpstreamAssetEvents({ dagId, dagRunId }) as TData, ...options });
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
export const useDagRunServiceGetDagRunsSuspense = <TData = Common.DagRunServiceGetDagRunsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, dagVersion, endDateGt, endDateGte, endDateLt, endDateLte, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, runType, startDateGt, startDateGte, startDateLt, startDateLte, state, triggeringUserNamePattern, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte }: {
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
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseDagRunServiceGetDagRunsKeyFn({ dagId, dagVersion, endDateGt, endDateGte, endDateLt, endDateLte, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, runType, startDateGt, startDateGte, startDateLt, startDateLte, state, triggeringUserNamePattern, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte }, queryKey), queryFn: () => DagRunService.getDagRuns({ dagId, dagVersion, endDateGt, endDateGte, endDateLt, endDateLte, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, runType, startDateGt, startDateGte, startDateLt, startDateLte, state, triggeringUserNamePattern, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte }) as TData, ...options });
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
export const useDagRunServiceWaitDagRunUntilFinishedSuspense = <TData = Common.DagRunServiceWaitDagRunUntilFinishedDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, dagRunId, interval, result }: {
  dagId: string;
  dagRunId: string;
  interval: number;
  result?: string[];
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseDagRunServiceWaitDagRunUntilFinishedKeyFn({ dagId, dagRunId, interval, result }, queryKey), queryFn: () => DagRunService.waitDagRunUntilFinished({ dagId, dagRunId, interval, result }) as TData, ...options });
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
export const useExperimentalServiceWaitDagRunUntilFinishedSuspense = <TData = Common.ExperimentalServiceWaitDagRunUntilFinishedDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, dagRunId, interval, result }: {
  dagId: string;
  dagRunId: string;
  interval: number;
  result?: string[];
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseExperimentalServiceWaitDagRunUntilFinishedKeyFn({ dagId, dagRunId, interval, result }, queryKey), queryFn: () => ExperimentalService.waitDagRunUntilFinished({ dagId, dagRunId, interval, result }) as TData, ...options });
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
export const useDagSourceServiceGetDagSourceSuspense = <TData = Common.DagSourceServiceGetDagSourceDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ accept, dagId, versionNumber }: {
  accept?: "application/json" | "text/plain" | "*/*";
  dagId: string;
  versionNumber?: number;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseDagSourceServiceGetDagSourceKeyFn({ accept, dagId, versionNumber }, queryKey), queryFn: () => DagSourceService.getDagSource({ accept, dagId, versionNumber }) as TData, ...options });
/**
* Get Dag Stats
* Get Dag statistics.
* @param data The data for the request.
* @param data.dagIds
* @returns DagStatsCollectionResponse Successful Response
* @throws ApiError
*/
export const useDagStatsServiceGetDagStatsSuspense = <TData = Common.DagStatsServiceGetDagStatsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagIds }: {
  dagIds?: string[];
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseDagStatsServiceGetDagStatsKeyFn({ dagIds }, queryKey), queryFn: () => DagStatsService.getDagStats({ dagIds }) as TData, ...options });
/**
* Get Dag Reports
* Get DAG report.
* @param data The data for the request.
* @param data.subdir
* @returns unknown Successful Response
* @throws ApiError
*/
export const useDagReportServiceGetDagReportsSuspense = <TData = Common.DagReportServiceGetDagReportsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ subdir }: {
  subdir: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseDagReportServiceGetDagReportsKeyFn({ subdir }, queryKey), queryFn: () => DagReportService.getDagReports({ subdir }) as TData, ...options });
/**
* Get Config
* @param data The data for the request.
* @param data.section
* @param data.accept
* @returns Config Successful Response
* @throws ApiError
*/
export const useConfigServiceGetConfigSuspense = <TData = Common.ConfigServiceGetConfigDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ accept, section }: {
  accept?: "application/json" | "text/plain" | "*/*";
  section?: string;
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseConfigServiceGetConfigKeyFn({ accept, section }, queryKey), queryFn: () => ConfigService.getConfig({ accept, section }) as TData, ...options });
/**
* Get Config Value
* @param data The data for the request.
* @param data.section
* @param data.option
* @param data.accept
* @returns Config Successful Response
* @throws ApiError
*/
export const useConfigServiceGetConfigValueSuspense = <TData = Common.ConfigServiceGetConfigValueDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ accept, option, section }: {
  accept?: "application/json" | "text/plain" | "*/*";
  option: string;
  section: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseConfigServiceGetConfigValueKeyFn({ accept, option, section }, queryKey), queryFn: () => ConfigService.getConfigValue({ accept, option, section }) as TData, ...options });
/**
* Get Configs
* Get configs for UI.
* @returns ConfigResponse Successful Response
* @throws ApiError
*/
export const useConfigServiceGetConfigsSuspense = <TData = Common.ConfigServiceGetConfigsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>(queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseConfigServiceGetConfigsKeyFn(queryKey), queryFn: () => ConfigService.getConfigs() as TData, ...options });
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
export const useDagWarningServiceListDagWarningsSuspense = <TData = Common.DagWarningServiceListDagWarningsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, limit, offset, orderBy, warningType }: {
  dagId?: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
  warningType?: DagWarningType;
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseDagWarningServiceListDagWarningsKeyFn({ dagId, limit, offset, orderBy, warningType }, queryKey), queryFn: () => DagWarningService.listDagWarnings({ dagId, limit, offset, orderBy, warningType }) as TData, ...options });
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
* @param data.orderBy
* @param data.isFavorite
* @returns DAGCollectionResponse Successful Response
* @throws ApiError
*/
export const useDagServiceGetDagsSuspense = <TData = Common.DagServiceGetDagsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagIdPattern, dagRunEndDateGt, dagRunEndDateGte, dagRunEndDateLt, dagRunEndDateLte, dagRunStartDateGt, dagRunStartDateGte, dagRunStartDateLt, dagRunStartDateLte, dagRunState, excludeStale, hasAssetSchedule, hasImportErrors, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode }: {
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
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseDagServiceGetDagsKeyFn({ assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagIdPattern, dagRunEndDateGt, dagRunEndDateGte, dagRunEndDateLt, dagRunEndDateLte, dagRunStartDateGt, dagRunStartDateGte, dagRunStartDateLt, dagRunStartDateLte, dagRunState, excludeStale, hasAssetSchedule, hasImportErrors, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode }, queryKey), queryFn: () => DagService.getDags({ assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagIdPattern, dagRunEndDateGt, dagRunEndDateGte, dagRunEndDateLt, dagRunEndDateLte, dagRunStartDateGt, dagRunStartDateGte, dagRunStartDateLt, dagRunStartDateLte, dagRunState, excludeStale, hasAssetSchedule, hasImportErrors, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode }) as TData, ...options });
/**
* Get Dag
* Get basic information about a DAG.
* @param data The data for the request.
* @param data.dagId
* @returns DAGResponse Successful Response
* @throws ApiError
*/
export const useDagServiceGetDagSuspense = <TData = Common.DagServiceGetDagDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId }: {
  dagId: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseDagServiceGetDagKeyFn({ dagId }, queryKey), queryFn: () => DagService.getDag({ dagId }) as TData, ...options });
/**
* Get Dag Details
* Get details of DAG.
* @param data The data for the request.
* @param data.dagId
* @returns DAGDetailsResponse Successful Response
* @throws ApiError
*/
export const useDagServiceGetDagDetailsSuspense = <TData = Common.DagServiceGetDagDetailsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId }: {
  dagId: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseDagServiceGetDagDetailsKeyFn({ dagId }, queryKey), queryFn: () => DagService.getDagDetails({ dagId }) as TData, ...options });
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
export const useDagServiceGetDagTagsSuspense = <TData = Common.DagServiceGetDagTagsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ limit, offset, orderBy, tagNamePattern }: {
  limit?: number;
  offset?: number;
  orderBy?: string[];
  tagNamePattern?: string;
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseDagServiceGetDagTagsKeyFn({ limit, offset, orderBy, tagNamePattern }, queryKey), queryFn: () => DagService.getDagTags({ limit, offset, orderBy, tagNamePattern }) as TData, ...options });
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
* @param data.orderBy
* @param data.isFavorite
* @param data.hasAssetSchedule Filter Dags with asset-based scheduling
* @param data.assetDependency Filter Dags by asset dependency (name or URI)
* @param data.hasPendingActions
* @returns DAGWithLatestDagRunsCollectionResponse Successful Response
* @throws ApiError
*/
export const useDagServiceGetDagsUiSuspense = <TData = Common.DagServiceGetDagsUiDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagIdPattern, dagIds, dagRunsLimit, excludeStale, hasAssetSchedule, hasImportErrors, hasPendingActions, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode }: {
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
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseDagServiceGetDagsUiKeyFn({ assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagIdPattern, dagIds, dagRunsLimit, excludeStale, hasAssetSchedule, hasImportErrors, hasPendingActions, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode }, queryKey), queryFn: () => DagService.getDagsUi({ assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagIdPattern, dagIds, dagRunsLimit, excludeStale, hasAssetSchedule, hasImportErrors, hasPendingActions, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode }) as TData, ...options });
/**
* Get Latest Run Info
* Get latest run.
* @param data The data for the request.
* @param data.dagId
* @returns unknown Successful Response
* @throws ApiError
*/
export const useDagServiceGetLatestRunInfoSuspense = <TData = Common.DagServiceGetLatestRunInfoDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId }: {
  dagId: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseDagServiceGetLatestRunInfoKeyFn({ dagId }, queryKey), queryFn: () => DagService.getLatestRunInfo({ dagId }) as TData, ...options });
/**
* Get Event Log
* @param data The data for the request.
* @param data.eventLogId
* @returns EventLogResponse Successful Response
* @throws ApiError
*/
export const useEventLogServiceGetEventLogSuspense = <TData = Common.EventLogServiceGetEventLogDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ eventLogId }: {
  eventLogId: number;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseEventLogServiceGetEventLogKeyFn({ eventLogId }, queryKey), queryFn: () => EventLogService.getEventLog({ eventLogId }) as TData, ...options });
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
export const useEventLogServiceGetEventLogsSuspense = <TData = Common.EventLogServiceGetEventLogsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ after, before, dagId, dagIdPattern, event, eventPattern, excludedEvents, includedEvents, limit, mapIndex, offset, orderBy, owner, ownerPattern, runId, runIdPattern, taskId, taskIdPattern, tryNumber }: {
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
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseEventLogServiceGetEventLogsKeyFn({ after, before, dagId, dagIdPattern, event, eventPattern, excludedEvents, includedEvents, limit, mapIndex, offset, orderBy, owner, ownerPattern, runId, runIdPattern, taskId, taskIdPattern, tryNumber }, queryKey), queryFn: () => EventLogService.getEventLogs({ after, before, dagId, dagIdPattern, event, eventPattern, excludedEvents, includedEvents, limit, mapIndex, offset, orderBy, owner, ownerPattern, runId, runIdPattern, taskId, taskIdPattern, tryNumber }) as TData, ...options });
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
export const useExtraLinksServiceGetExtraLinksSuspense = <TData = Common.ExtraLinksServiceGetExtraLinksDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, dagRunId, mapIndex, taskId }: {
  dagId: string;
  dagRunId: string;
  mapIndex?: number;
  taskId: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseExtraLinksServiceGetExtraLinksKeyFn({ dagId, dagRunId, mapIndex, taskId }, queryKey), queryFn: () => ExtraLinksService.getExtraLinks({ dagId, dagRunId, mapIndex, taskId }) as TData, ...options });
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
export const useTaskInstanceServiceGetExtraLinksSuspense = <TData = Common.TaskInstanceServiceGetExtraLinksDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, dagRunId, mapIndex, taskId }: {
  dagId: string;
  dagRunId: string;
  mapIndex?: number;
  taskId: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseTaskInstanceServiceGetExtraLinksKeyFn({ dagId, dagRunId, mapIndex, taskId }, queryKey), queryFn: () => TaskInstanceService.getExtraLinks({ dagId, dagRunId, mapIndex, taskId }) as TData, ...options });
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
export const useTaskInstanceServiceGetTaskInstanceSuspense = <TData = Common.TaskInstanceServiceGetTaskInstanceDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, dagRunId, taskId }: {
  dagId: string;
  dagRunId: string;
  taskId: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseTaskInstanceServiceGetTaskInstanceKeyFn({ dagId, dagRunId, taskId }, queryKey), queryFn: () => TaskInstanceService.getTaskInstance({ dagId, dagRunId, taskId }) as TData, ...options });
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
export const useTaskInstanceServiceGetMappedTaskInstancesSuspense = <TData = Common.TaskInstanceServiceGetMappedTaskInstancesDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, offset, operator, orderBy, pool, queue, runAfterGt, runAfterGte, runAfterLt, runAfterLte, startDateGt, startDateGte, startDateLt, startDateLte, state, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }: {
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
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstancesKeyFn({ dagId, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, offset, operator, orderBy, pool, queue, runAfterGt, runAfterGte, runAfterLt, runAfterLte, startDateGt, startDateGte, startDateLt, startDateLte, state, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }, queryKey), queryFn: () => TaskInstanceService.getMappedTaskInstances({ dagId, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, offset, operator, orderBy, pool, queue, runAfterGt, runAfterGte, runAfterLt, runAfterLte, startDateGt, startDateGte, startDateLt, startDateLte, state, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }) as TData, ...options });
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
export const useTaskInstanceServiceGetTaskInstanceDependenciesByMapIndexSuspense = <TData = Common.TaskInstanceServiceGetTaskInstanceDependenciesByMapIndexDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, dagRunId, mapIndex, taskId }: {
  dagId: string;
  dagRunId: string;
  mapIndex: number;
  taskId: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseTaskInstanceServiceGetTaskInstanceDependenciesByMapIndexKeyFn({ dagId, dagRunId, mapIndex, taskId }, queryKey), queryFn: () => TaskInstanceService.getTaskInstanceDependenciesByMapIndex({ dagId, dagRunId, mapIndex, taskId }) as TData, ...options });
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
export const useTaskInstanceServiceGetTaskInstanceDependenciesSuspense = <TData = Common.TaskInstanceServiceGetTaskInstanceDependenciesDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, dagRunId, mapIndex, taskId }: {
  dagId: string;
  dagRunId: string;
  mapIndex?: number;
  taskId: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseTaskInstanceServiceGetTaskInstanceDependenciesKeyFn({ dagId, dagRunId, mapIndex, taskId }, queryKey), queryFn: () => TaskInstanceService.getTaskInstanceDependencies({ dagId, dagRunId, mapIndex, taskId }) as TData, ...options });
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
export const useTaskInstanceServiceGetTaskInstanceTriesSuspense = <TData = Common.TaskInstanceServiceGetTaskInstanceTriesDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, dagRunId, mapIndex, taskId }: {
  dagId: string;
  dagRunId: string;
  mapIndex?: number;
  taskId: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseTaskInstanceServiceGetTaskInstanceTriesKeyFn({ dagId, dagRunId, mapIndex, taskId }, queryKey), queryFn: () => TaskInstanceService.getTaskInstanceTries({ dagId, dagRunId, mapIndex, taskId }) as TData, ...options });
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
export const useTaskInstanceServiceGetMappedTaskInstanceTriesSuspense = <TData = Common.TaskInstanceServiceGetMappedTaskInstanceTriesDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, dagRunId, mapIndex, taskId }: {
  dagId: string;
  dagRunId: string;
  mapIndex: number;
  taskId: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstanceTriesKeyFn({ dagId, dagRunId, mapIndex, taskId }, queryKey), queryFn: () => TaskInstanceService.getMappedTaskInstanceTries({ dagId, dagRunId, mapIndex, taskId }) as TData, ...options });
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
export const useTaskInstanceServiceGetMappedTaskInstanceSuspense = <TData = Common.TaskInstanceServiceGetMappedTaskInstanceDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, dagRunId, mapIndex, taskId }: {
  dagId: string;
  dagRunId: string;
  mapIndex: number;
  taskId: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstanceKeyFn({ dagId, dagRunId, mapIndex, taskId }, queryKey), queryFn: () => TaskInstanceService.getMappedTaskInstance({ dagId, dagRunId, mapIndex, taskId }) as TData, ...options });
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
export const useTaskInstanceServiceGetTaskInstancesSuspense = <TData = Common.TaskInstanceServiceGetTaskInstancesDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, offset, operator, orderBy, pool, queue, runAfterGt, runAfterGte, runAfterLt, runAfterLte, startDateGt, startDateGte, startDateLt, startDateLte, state, taskDisplayNamePattern, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }: {
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
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseTaskInstanceServiceGetTaskInstancesKeyFn({ dagId, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, offset, operator, orderBy, pool, queue, runAfterGt, runAfterGte, runAfterLt, runAfterLte, startDateGt, startDateGte, startDateLt, startDateLte, state, taskDisplayNamePattern, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }, queryKey), queryFn: () => TaskInstanceService.getTaskInstances({ dagId, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, offset, operator, orderBy, pool, queue, runAfterGt, runAfterGte, runAfterLt, runAfterLte, startDateGt, startDateGte, startDateLt, startDateLte, state, taskDisplayNamePattern, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }) as TData, ...options });
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
export const useTaskInstanceServiceGetTaskInstanceTryDetailsSuspense = <TData = Common.TaskInstanceServiceGetTaskInstanceTryDetailsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, dagRunId, mapIndex, taskId, taskTryNumber }: {
  dagId: string;
  dagRunId: string;
  mapIndex?: number;
  taskId: string;
  taskTryNumber: number;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseTaskInstanceServiceGetTaskInstanceTryDetailsKeyFn({ dagId, dagRunId, mapIndex, taskId, taskTryNumber }, queryKey), queryFn: () => TaskInstanceService.getTaskInstanceTryDetails({ dagId, dagRunId, mapIndex, taskId, taskTryNumber }) as TData, ...options });
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
export const useTaskInstanceServiceGetMappedTaskInstanceTryDetailsSuspense = <TData = Common.TaskInstanceServiceGetMappedTaskInstanceTryDetailsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, dagRunId, mapIndex, taskId, taskTryNumber }: {
  dagId: string;
  dagRunId: string;
  mapIndex: number;
  taskId: string;
  taskTryNumber: number;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstanceTryDetailsKeyFn({ dagId, dagRunId, mapIndex, taskId, taskTryNumber }, queryKey), queryFn: () => TaskInstanceService.getMappedTaskInstanceTryDetails({ dagId, dagRunId, mapIndex, taskId, taskTryNumber }) as TData, ...options });
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
export const useTaskInstanceServiceGetLogSuspense = <TData = Common.TaskInstanceServiceGetLogDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ accept, dagId, dagRunId, fullContent, mapIndex, taskId, token, tryNumber }: {
  accept?: "application/json" | "*/*" | "application/x-ndjson";
  dagId: string;
  dagRunId: string;
  fullContent?: boolean;
  mapIndex?: number;
  taskId: string;
  token?: string;
  tryNumber: number;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseTaskInstanceServiceGetLogKeyFn({ accept, dagId, dagRunId, fullContent, mapIndex, taskId, token, tryNumber }, queryKey), queryFn: () => TaskInstanceService.getLog({ accept, dagId, dagRunId, fullContent, mapIndex, taskId, token, tryNumber }) as TData, ...options });
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
export const useTaskInstanceServiceGetExternalLogUrlSuspense = <TData = Common.TaskInstanceServiceGetExternalLogUrlDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, dagRunId, mapIndex, taskId, tryNumber }: {
  dagId: string;
  dagRunId: string;
  mapIndex?: number;
  taskId: string;
  tryNumber: number;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseTaskInstanceServiceGetExternalLogUrlKeyFn({ dagId, dagRunId, mapIndex, taskId, tryNumber }, queryKey), queryFn: () => TaskInstanceService.getExternalLogUrl({ dagId, dagRunId, mapIndex, taskId, tryNumber }) as TData, ...options });
/**
* Get Import Error
* Get an import error.
* @param data The data for the request.
* @param data.importErrorId
* @returns ImportErrorResponse Successful Response
* @throws ApiError
*/
export const useImportErrorServiceGetImportErrorSuspense = <TData = Common.ImportErrorServiceGetImportErrorDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ importErrorId }: {
  importErrorId: number;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseImportErrorServiceGetImportErrorKeyFn({ importErrorId }, queryKey), queryFn: () => ImportErrorService.getImportError({ importErrorId }) as TData, ...options });
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
export const useImportErrorServiceGetImportErrorsSuspense = <TData = Common.ImportErrorServiceGetImportErrorsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ limit, offset, orderBy }: {
  limit?: number;
  offset?: number;
  orderBy?: string[];
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseImportErrorServiceGetImportErrorsKeyFn({ limit, offset, orderBy }, queryKey), queryFn: () => ImportErrorService.getImportErrors({ limit, offset, orderBy }) as TData, ...options });
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
export const useJobServiceGetJobsSuspense = <TData = Common.JobServiceGetJobsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ endDateGt, endDateGte, endDateLt, endDateLte, executorClass, hostname, isAlive, jobState, jobType, limit, offset, orderBy, startDateGt, startDateGte, startDateLt, startDateLte }: {
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
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseJobServiceGetJobsKeyFn({ endDateGt, endDateGte, endDateLt, endDateLte, executorClass, hostname, isAlive, jobState, jobType, limit, offset, orderBy, startDateGt, startDateGte, startDateLt, startDateLte }, queryKey), queryFn: () => JobService.getJobs({ endDateGt, endDateGte, endDateLt, endDateLte, executorClass, hostname, isAlive, jobState, jobType, limit, offset, orderBy, startDateGt, startDateGte, startDateLt, startDateLte }) as TData, ...options });
/**
* Get Plugins
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @returns PluginCollectionResponse Successful Response
* @throws ApiError
*/
export const usePluginServiceGetPluginsSuspense = <TData = Common.PluginServiceGetPluginsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ limit, offset }: {
  limit?: number;
  offset?: number;
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UsePluginServiceGetPluginsKeyFn({ limit, offset }, queryKey), queryFn: () => PluginService.getPlugins({ limit, offset }) as TData, ...options });
/**
* Import Errors
* @returns PluginImportErrorCollectionResponse Successful Response
* @throws ApiError
*/
export const usePluginServiceImportErrorsSuspense = <TData = Common.PluginServiceImportErrorsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>(queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UsePluginServiceImportErrorsKeyFn(queryKey), queryFn: () => PluginService.importErrors() as TData, ...options });
/**
* Get Pool
* Get a pool.
* @param data The data for the request.
* @param data.poolName
* @returns PoolResponse Successful Response
* @throws ApiError
*/
export const usePoolServiceGetPoolSuspense = <TData = Common.PoolServiceGetPoolDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ poolName }: {
  poolName: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UsePoolServiceGetPoolKeyFn({ poolName }, queryKey), queryFn: () => PoolService.getPool({ poolName }) as TData, ...options });
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
export const usePoolServiceGetPoolsSuspense = <TData = Common.PoolServiceGetPoolsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ limit, offset, orderBy, poolNamePattern }: {
  limit?: number;
  offset?: number;
  orderBy?: string[];
  poolNamePattern?: string;
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UsePoolServiceGetPoolsKeyFn({ limit, offset, orderBy, poolNamePattern }, queryKey), queryFn: () => PoolService.getPools({ limit, offset, orderBy, poolNamePattern }) as TData, ...options });
/**
* Get Providers
* Get providers.
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @returns ProviderCollectionResponse Successful Response
* @throws ApiError
*/
export const useProviderServiceGetProvidersSuspense = <TData = Common.ProviderServiceGetProvidersDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ limit, offset }: {
  limit?: number;
  offset?: number;
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseProviderServiceGetProvidersKeyFn({ limit, offset }, queryKey), queryFn: () => ProviderService.getProviders({ limit, offset }) as TData, ...options });
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
export const useXcomServiceGetXcomEntrySuspense = <TData = Common.XcomServiceGetXcomEntryDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, dagRunId, deserialize, mapIndex, stringify, taskId, xcomKey }: {
  dagId: string;
  dagRunId: string;
  deserialize?: boolean;
  mapIndex?: number;
  stringify?: boolean;
  taskId: string;
  xcomKey: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseXcomServiceGetXcomEntryKeyFn({ dagId, dagRunId, deserialize, mapIndex, stringify, taskId, xcomKey }, queryKey), queryFn: () => XcomService.getXcomEntry({ dagId, dagRunId, deserialize, mapIndex, stringify, taskId, xcomKey }) as TData, ...options });
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
export const useXcomServiceGetXcomEntriesSuspense = <TData = Common.XcomServiceGetXcomEntriesDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagDisplayNamePattern, dagId, dagRunId, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, mapIndexFilter, offset, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, taskId, taskIdPattern, xcomKey, xcomKeyPattern }: {
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
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseXcomServiceGetXcomEntriesKeyFn({ dagDisplayNamePattern, dagId, dagRunId, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, mapIndexFilter, offset, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, taskId, taskIdPattern, xcomKey, xcomKeyPattern }, queryKey), queryFn: () => XcomService.getXcomEntries({ dagDisplayNamePattern, dagId, dagRunId, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, mapIndexFilter, offset, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, taskId, taskIdPattern, xcomKey, xcomKeyPattern }) as TData, ...options });
/**
* Get Tasks
* Get tasks for DAG.
* @param data The data for the request.
* @param data.dagId
* @param data.orderBy
* @returns TaskCollectionResponse Successful Response
* @throws ApiError
*/
export const useTaskServiceGetTasksSuspense = <TData = Common.TaskServiceGetTasksDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, orderBy }: {
  dagId: string;
  orderBy?: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseTaskServiceGetTasksKeyFn({ dagId, orderBy }, queryKey), queryFn: () => TaskService.getTasks({ dagId, orderBy }) as TData, ...options });
/**
* Get Task
* Get simplified representation of a task.
* @param data The data for the request.
* @param data.dagId
* @param data.taskId
* @returns TaskResponse Successful Response
* @throws ApiError
*/
export const useTaskServiceGetTaskSuspense = <TData = Common.TaskServiceGetTaskDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, taskId }: {
  dagId: string;
  taskId: unknown;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseTaskServiceGetTaskKeyFn({ dagId, taskId }, queryKey), queryFn: () => TaskService.getTask({ dagId, taskId }) as TData, ...options });
/**
* Get Variable
* Get a variable entry.
* @param data The data for the request.
* @param data.variableKey
* @returns VariableResponse Successful Response
* @throws ApiError
*/
export const useVariableServiceGetVariableSuspense = <TData = Common.VariableServiceGetVariableDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ variableKey }: {
  variableKey: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseVariableServiceGetVariableKeyFn({ variableKey }, queryKey), queryFn: () => VariableService.getVariable({ variableKey }) as TData, ...options });
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
export const useVariableServiceGetVariablesSuspense = <TData = Common.VariableServiceGetVariablesDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ limit, offset, orderBy, variableKeyPattern }: {
  limit?: number;
  offset?: number;
  orderBy?: string[];
  variableKeyPattern?: string;
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseVariableServiceGetVariablesKeyFn({ limit, offset, orderBy, variableKeyPattern }, queryKey), queryFn: () => VariableService.getVariables({ limit, offset, orderBy, variableKeyPattern }) as TData, ...options });
/**
* Get Dag Version
* Get one Dag Version.
* @param data The data for the request.
* @param data.dagId
* @param data.versionNumber
* @returns DagVersionResponse Successful Response
* @throws ApiError
*/
export const useDagVersionServiceGetDagVersionSuspense = <TData = Common.DagVersionServiceGetDagVersionDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, versionNumber }: {
  dagId: string;
  versionNumber: number;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseDagVersionServiceGetDagVersionKeyFn({ dagId, versionNumber }, queryKey), queryFn: () => DagVersionService.getDagVersion({ dagId, versionNumber }) as TData, ...options });
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
export const useDagVersionServiceGetDagVersionsSuspense = <TData = Common.DagVersionServiceGetDagVersionsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ bundleName, bundleVersion, dagId, limit, offset, orderBy, versionNumber }: {
  bundleName?: string;
  bundleVersion?: string;
  dagId: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
  versionNumber?: number;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseDagVersionServiceGetDagVersionsKeyFn({ bundleName, bundleVersion, dagId, limit, offset, orderBy, versionNumber }, queryKey), queryFn: () => DagVersionService.getDagVersions({ bundleName, bundleVersion, dagId, limit, offset, orderBy, versionNumber }) as TData, ...options });
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
export const useHumanInTheLoopServiceGetHitlDetailSuspense = <TData = Common.HumanInTheLoopServiceGetHitlDetailDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, dagRunId, mapIndex, taskId }: {
  dagId: string;
  dagRunId: string;
  mapIndex?: number;
  taskId: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseHumanInTheLoopServiceGetHitlDetailKeyFn({ dagId, dagRunId, mapIndex, taskId }, queryKey), queryFn: () => HumanInTheLoopService.getHitlDetail({ dagId, dagRunId, mapIndex, taskId }) as TData, ...options });
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
* @param data.respondedByUserId
* @param data.respondedByUserName
* @param data.subjectSearch SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @param data.bodySearch SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @returns HITLDetailCollection Successful Response
* @throws ApiError
*/
export const useHumanInTheLoopServiceGetHitlDetailsSuspense = <TData = Common.HumanInTheLoopServiceGetHitlDetailsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ bodySearch, dagId, dagIdPattern, dagRunId, limit, offset, orderBy, respondedByUserId, respondedByUserName, responseReceived, state, subjectSearch, taskId, taskIdPattern }: {
  bodySearch?: string;
  dagId?: string;
  dagIdPattern?: string;
  dagRunId?: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
  respondedByUserId?: string[];
  respondedByUserName?: string[];
  responseReceived?: boolean;
  state?: string[];
  subjectSearch?: string;
  taskId?: string;
  taskIdPattern?: string;
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseHumanInTheLoopServiceGetHitlDetailsKeyFn({ bodySearch, dagId, dagIdPattern, dagRunId, limit, offset, orderBy, respondedByUserId, respondedByUserName, responseReceived, state, subjectSearch, taskId, taskIdPattern }, queryKey), queryFn: () => HumanInTheLoopService.getHitlDetails({ bodySearch, dagId, dagIdPattern, dagRunId, limit, offset, orderBy, respondedByUserId, respondedByUserName, responseReceived, state, subjectSearch, taskId, taskIdPattern }) as TData, ...options });
/**
* Get Health
* @returns HealthInfoResponse Successful Response
* @throws ApiError
*/
export const useMonitorServiceGetHealthSuspense = <TData = Common.MonitorServiceGetHealthDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>(queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseMonitorServiceGetHealthKeyFn(queryKey), queryFn: () => MonitorService.getHealth() as TData, ...options });
/**
* Get Version
* Get version information.
* @returns VersionInfo Successful Response
* @throws ApiError
*/
export const useVersionServiceGetVersionSuspense = <TData = Common.VersionServiceGetVersionDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>(queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseVersionServiceGetVersionKeyFn(queryKey), queryFn: () => VersionService.getVersion() as TData, ...options });
/**
* Login
* Redirect to the login URL depending on the AuthManager configured.
* @param data The data for the request.
* @param data.next
* @returns unknown Successful Response
* @throws ApiError
*/
export const useLoginServiceLoginSuspense = <TData = Common.LoginServiceLoginDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ next }: {
  next?: string;
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseLoginServiceLoginKeyFn({ next }, queryKey), queryFn: () => LoginService.login({ next }) as TData, ...options });
/**
* Logout
* Logout the user.
* @param data The data for the request.
* @param data.next
* @returns unknown Successful Response
* @throws ApiError
*/
export const useLoginServiceLogoutSuspense = <TData = Common.LoginServiceLogoutDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ next }: {
  next?: string;
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseLoginServiceLogoutKeyFn({ next }, queryKey), queryFn: () => LoginService.logout({ next }) as TData, ...options });
/**
* Refresh
* Refresh the authentication token.
* @param data The data for the request.
* @param data.next
* @returns unknown Successful Response
* @throws ApiError
*/
export const useLoginServiceRefreshSuspense = <TData = Common.LoginServiceRefreshDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ next }: {
  next?: string;
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseLoginServiceRefreshKeyFn({ next }, queryKey), queryFn: () => LoginService.refresh({ next }) as TData, ...options });
/**
* Get Auth Menus
* @returns MenuItemCollectionResponse Successful Response
* @throws ApiError
*/
export const useAuthLinksServiceGetAuthMenusSuspense = <TData = Common.AuthLinksServiceGetAuthMenusDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>(queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseAuthLinksServiceGetAuthMenusKeyFn(queryKey), queryFn: () => AuthLinksService.getAuthMenus() as TData, ...options });
/**
* Get Dependencies
* Dependencies graph.
* @param data The data for the request.
* @param data.nodeId
* @returns BaseGraphResponse Successful Response
* @throws ApiError
*/
export const useDependenciesServiceGetDependenciesSuspense = <TData = Common.DependenciesServiceGetDependenciesDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ nodeId }: {
  nodeId?: string;
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseDependenciesServiceGetDependenciesKeyFn({ nodeId }, queryKey), queryFn: () => DependenciesService.getDependencies({ nodeId }) as TData, ...options });
/**
* Historical Metrics
* Return cluster activity historical metrics.
* @param data The data for the request.
* @param data.startDate
* @param data.endDate
* @returns HistoricalMetricDataResponse Successful Response
* @throws ApiError
*/
export const useDashboardServiceHistoricalMetricsSuspense = <TData = Common.DashboardServiceHistoricalMetricsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ endDate, startDate }: {
  endDate?: string;
  startDate: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseDashboardServiceHistoricalMetricsKeyFn({ endDate, startDate }, queryKey), queryFn: () => DashboardService.historicalMetrics({ endDate, startDate }) as TData, ...options });
/**
* Dag Stats
* Return basic DAG stats with counts of DAGs in various states.
* @returns DashboardDagStatsResponse Successful Response
* @throws ApiError
*/
export const useDashboardServiceDagStatsSuspense = <TData = Common.DashboardServiceDagStatsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>(queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseDashboardServiceDagStatsKeyFn(queryKey), queryFn: () => DashboardService.dagStats() as TData, ...options });
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
export const useStructureServiceStructureDataSuspense = <TData = Common.StructureServiceStructureDataDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, externalDependencies, includeDownstream, includeUpstream, root, versionNumber }: {
  dagId: string;
  externalDependencies?: boolean;
  includeDownstream?: boolean;
  includeUpstream?: boolean;
  root?: string;
  versionNumber?: number;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseStructureServiceStructureDataKeyFn({ dagId, externalDependencies, includeDownstream, includeUpstream, root, versionNumber }, queryKey), queryFn: () => StructureService.structureData({ dagId, externalDependencies, includeDownstream, includeUpstream, root, versionNumber }) as TData, ...options });
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
* @param data.runType
* @param data.triggeringUser SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @returns GridNodeResponse Successful Response
* @throws ApiError
*/
export const useGridServiceGetDagStructureSuspense = <TData = Common.GridServiceGetDagStructureDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, limit, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runType, triggeringUser }: {
  dagId: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
  runAfterGt?: string;
  runAfterGte?: string;
  runAfterLt?: string;
  runAfterLte?: string;
  runType?: string[];
  triggeringUser?: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseGridServiceGetDagStructureKeyFn({ dagId, limit, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runType, triggeringUser }, queryKey), queryFn: () => GridService.getDagStructure({ dagId, limit, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runType, triggeringUser }) as TData, ...options });
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
* @param data.runType
* @param data.triggeringUser SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). Regular expressions are **not** supported.
* @returns GridRunsResponse Successful Response
* @throws ApiError
*/
export const useGridServiceGetGridRunsSuspense = <TData = Common.GridServiceGetGridRunsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, limit, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runType, triggeringUser }: {
  dagId: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
  runAfterGt?: string;
  runAfterGte?: string;
  runAfterLt?: string;
  runAfterLte?: string;
  runType?: string[];
  triggeringUser?: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseGridServiceGetGridRunsKeyFn({ dagId, limit, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runType, triggeringUser }, queryKey), queryFn: () => GridService.getGridRuns({ dagId, limit, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runType, triggeringUser }) as TData, ...options });
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
export const useGridServiceGetGridTiSummariesSuspense = <TData = Common.GridServiceGetGridTiSummariesDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, runId }: {
  dagId: string;
  runId: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseGridServiceGetGridTiSummariesKeyFn({ dagId, runId }, queryKey), queryFn: () => GridService.getGridTiSummaries({ dagId, runId }) as TData, ...options });
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
export const useCalendarServiceGetCalendarSuspense = <TData = Common.CalendarServiceGetCalendarDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, granularity, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte }: {
  dagId: string;
  granularity?: "hourly" | "daily";
  logicalDateGt?: string;
  logicalDateGte?: string;
  logicalDateLt?: string;
  logicalDateLte?: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseCalendarServiceGetCalendarKeyFn({ dagId, granularity, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte }, queryKey), queryFn: () => CalendarService.getCalendar({ dagId, granularity, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte }) as TData, ...options });
