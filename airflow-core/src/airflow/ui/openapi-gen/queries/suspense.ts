// generated with @7nohe/openapi-react-query-codegen@1.6.2 

import { UseQueryOptions, useSuspenseQuery } from "@tanstack/react-query";
import { AssetService, AuthLinksService, BackfillService, CalendarService, ConfigService, ConnectionService, DagRunService, DagService, DagSourceService, DagStatsService, DagVersionService, DagWarningService, DashboardService, DeadlinesService, DependenciesService, EventLogService, ExperimentalService, ExtraLinksService, GanttService, GridService, ImportErrorService, JobService, LoginService, MonitorService, PartitionedDagRunService, PluginService, PoolService, ProviderService, StructureService, TaskInstanceService, TaskService, TeamsService, VariableService, VersionService, XcomService } from "../requests/services.gen";
import { DagRunState, DagWarningType } from "../requests/types.gen";
import * as Common from "./common";
/**
* Get Assets
* Get assets.
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @param data.namePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``name_prefix_pattern`` parameter when possible.
* @param data.namePrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @param data.uriPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``uri_prefix_pattern`` parameter when possible.
* @param data.uriPrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @param data.dagIds
* @param data.onlyActive
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `id, name, uri, created_at, updated_at`
* @returns AssetCollectionResponse Successful Response
* @throws ApiError
*/
export const useAssetServiceGetAssetsSuspense = <TData = Common.AssetServiceGetAssetsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagIds, limit, namePattern, namePrefixPattern, offset, onlyActive, orderBy, uriPattern, uriPrefixPattern }: {
  dagIds?: string[];
  limit?: number;
  namePattern?: string;
  namePrefixPattern?: string;
  offset?: number;
  onlyActive?: boolean;
  orderBy?: string[];
  uriPattern?: string;
  uriPrefixPattern?: string;
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseAssetServiceGetAssetsKeyFn({ dagIds, limit, namePattern, namePrefixPattern, offset, onlyActive, orderBy, uriPattern, uriPrefixPattern }, queryKey), queryFn: () => AssetService.getAssets({ dagIds, limit, namePattern, namePrefixPattern, offset, onlyActive, orderBy, uriPattern, uriPrefixPattern }) as TData, ...options });
/**
* Get Asset Aliases
* Get asset aliases.
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @param data.namePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``name_prefix_pattern`` parameter when possible.
* @param data.namePrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `id, name`
* @returns AssetAliasCollectionResponse Successful Response
* @throws ApiError
*/
export const useAssetServiceGetAssetAliasesSuspense = <TData = Common.AssetServiceGetAssetAliasesDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ limit, namePattern, namePrefixPattern, offset, orderBy }: {
  limit?: number;
  namePattern?: string;
  namePrefixPattern?: string;
  offset?: number;
  orderBy?: string[];
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseAssetServiceGetAssetAliasesKeyFn({ limit, namePattern, namePrefixPattern, offset, orderBy }, queryKey), queryFn: () => AssetService.getAssetAliases({ limit, namePattern, namePrefixPattern, offset, orderBy }) as TData, ...options });
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
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `source_task_id, source_dag_id, source_run_id, source_map_index, timestamp`
* @param data.assetId
* @param data.sourceDagId
* @param data.sourceTaskId
* @param data.sourceRunId
* @param data.sourceMapIndex
* @param data.namePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``name_prefix_pattern`` parameter when possible.
* @param data.namePrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @param data.timestampGte
* @param data.timestampGt
* @param data.timestampLte
* @param data.timestampLt
* @returns AssetEventCollectionResponse Successful Response
* @throws ApiError
*/
export const useAssetServiceGetAssetEventsSuspense = <TData = Common.AssetServiceGetAssetEventsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ assetId, limit, namePattern, namePrefixPattern, offset, orderBy, sourceDagId, sourceMapIndex, sourceRunId, sourceTaskId, timestampGt, timestampGte, timestampLt, timestampLte }: {
  assetId?: number;
  limit?: number;
  namePattern?: string;
  namePrefixPattern?: string;
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
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseAssetServiceGetAssetEventsKeyFn({ assetId, limit, namePattern, namePrefixPattern, offset, orderBy, sourceDagId, sourceMapIndex, sourceRunId, sourceTaskId, timestampGt, timestampGte, timestampLt, timestampLte }, queryKey), queryFn: () => AssetService.getAssetEvents({ assetId, limit, namePattern, namePrefixPattern, offset, orderBy, sourceDagId, sourceMapIndex, sourceRunId, sourceTaskId, timestampGt, timestampGte, timestampLt, timestampLte }) as TData, ...options });
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
* Get queued asset events for a Dag.
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
* Get a queued asset event for a Dag.
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
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `id`
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
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `id`
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
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `conn_id, conn_type, description, host, port, id, team_name, connection_id`
* @param data.connectionIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``connection_id_prefix_pattern`` parameter when possible.
* @param data.connectionIdPrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @returns ConnectionCollectionResponse Successful Response
* @throws ApiError
*/
export const useConnectionServiceGetConnectionsSuspense = <TData = Common.ConnectionServiceGetConnectionsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ connectionIdPattern, connectionIdPrefixPattern, limit, offset, orderBy }: {
  connectionIdPattern?: string;
  connectionIdPrefixPattern?: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseConnectionServiceGetConnectionsKeyFn({ connectionIdPattern, connectionIdPrefixPattern, limit, offset, orderBy }, queryKey), queryFn: () => ConnectionService.getConnections({ connectionIdPattern, connectionIdPrefixPattern, limit, offset, orderBy }) as TData, ...options });
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
* Get all Dag Runs.
*
* This endpoint allows specifying `~` as the dag_id to retrieve Dag Runs for all Dags.
*
* Supports two pagination modes:
*
* **Offset (default):** use `limit` and `offset` query parameters. Returns `total_entries`.
*
* **Cursor:** pass `cursor` (empty string for the first page, then `next_cursor` from the response).
* When `cursor` is provided, `offset` is ignored and `total_entries` is not returned.
* ``next_cursor`` is ``null`` when there are no more pages; ``previous_cursor`` is ``null``
* on the first page.
* @param data The data for the request.
* @param data.dagId
* @param data.cursor Cursor for keyset-based pagination. Pass an empty string for the first page, then use ``next_cursor`` from the response. When ``cursor`` is provided, ``offset`` is ignored.
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
* @param data.bundleVersion
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `id, state, dag_id, run_id, logical_date, run_after, start_date, end_date, updated_at, conf, duration, dag_run_id`
* @param data.runIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``run_id_prefix_pattern`` parameter when possible.
* @param data.runIdPrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @param data.triggeringUserNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``triggering_user_name_prefix_pattern`` parameter when possible.
* @param data.triggeringUserNamePrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @param data.dagIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``dag_id_prefix_pattern`` parameter when possible.
* @param data.dagIdPrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @param data.partitionKeyPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``partition_key_prefix_pattern`` parameter when possible.
* @param data.partitionKeyPrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @param data.consumingAssetPattern Filter by consuming asset name or URI using pattern matching
* @returns DAGRunCollectionResponse Successful Response
* @throws ApiError
*/
export const useDagRunServiceGetDagRunsSuspense = <TData = Common.DagRunServiceGetDagRunsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ bundleVersion, confContains, consumingAssetPattern, cursor, dagId, dagIdPattern, dagIdPrefixPattern, dagVersion, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, offset, orderBy, partitionKeyPattern, partitionKeyPrefixPattern, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, runIdPrefixPattern, runType, startDateGt, startDateGte, startDateLt, startDateLte, state, triggeringUserNamePattern, triggeringUserNamePrefixPattern, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte }: {
  bundleVersion?: string;
  confContains?: string;
  consumingAssetPattern?: string;
  cursor?: string;
  dagId: string;
  dagIdPattern?: string;
  dagIdPrefixPattern?: string;
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
  partitionKeyPattern?: string;
  partitionKeyPrefixPattern?: string;
  runAfterGt?: string;
  runAfterGte?: string;
  runAfterLt?: string;
  runAfterLte?: string;
  runIdPattern?: string;
  runIdPrefixPattern?: string;
  runType?: string[];
  startDateGt?: string;
  startDateGte?: string;
  startDateLt?: string;
  startDateLte?: string;
  state?: string[];
  triggeringUserNamePattern?: string;
  triggeringUserNamePrefixPattern?: string;
  updatedAtGt?: string;
  updatedAtGte?: string;
  updatedAtLt?: string;
  updatedAtLte?: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseDagRunServiceGetDagRunsKeyFn({ bundleVersion, confContains, consumingAssetPattern, cursor, dagId, dagIdPattern, dagIdPrefixPattern, dagVersion, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, offset, orderBy, partitionKeyPattern, partitionKeyPrefixPattern, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, runIdPrefixPattern, runType, startDateGt, startDateGte, startDateLt, startDateLte, state, triggeringUserNamePattern, triggeringUserNamePrefixPattern, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte }, queryKey), queryFn: () => DagRunService.getDagRuns({ bundleVersion, confContains, consumingAssetPattern, cursor, dagId, dagIdPattern, dagIdPrefixPattern, dagVersion, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, offset, orderBy, partitionKeyPattern, partitionKeyPrefixPattern, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, runIdPrefixPattern, runType, startDateGt, startDateGte, startDateLt, startDateLte, state, triggeringUserNamePattern, triggeringUserNamePrefixPattern, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte }) as TData, ...options });
/**
* Experimental: Wait for a dag run to complete, and return task results if requested.
* 🚧 This is an experimental endpoint and may change or be removed without notice.Successful response are streamed as newline-delimited JSON (NDJSON). Each line is a JSON object representing the Dag run state.
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
* 🚧 This is an experimental endpoint and may change or be removed without notice.Successful response are streamed as newline-delimited JSON (NDJSON). Each line is a JSON object representing the Dag run state.
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
* Get a list of Dag warnings.
* @param data The data for the request.
* @param data.dagId
* @param data.warningType
* @param data.limit
* @param data.offset
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `dag_id, warning_type, message, timestamp`
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
* Get all Dags.
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @param data.tags
* @param data.tagsMatchMode
* @param data.owners
* @param data.dagIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``dag_id_prefix_pattern`` parameter when possible.
* @param data.dagIdPrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @param data.dagDisplayNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``dag_display_name_prefix_pattern`` parameter when possible.
* @param data.dagDisplayNamePrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
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
* @param data.timetableType
* @returns DAGCollectionResponse Successful Response
* @throws ApiError
*/
export const useDagServiceGetDagsSuspense = <TData = Common.DagServiceGetDagsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagDisplayNamePrefixPattern, dagIdPattern, dagIdPrefixPattern, dagRunEndDateGt, dagRunEndDateGte, dagRunEndDateLt, dagRunEndDateLte, dagRunStartDateGt, dagRunStartDateGte, dagRunStartDateLt, dagRunStartDateLte, dagRunState, excludeStale, hasAssetSchedule, hasImportErrors, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode, timetableType }: {
  assetDependency?: string;
  bundleName?: string;
  bundleVersion?: string;
  dagDisplayNamePattern?: string;
  dagDisplayNamePrefixPattern?: string;
  dagIdPattern?: string;
  dagIdPrefixPattern?: string;
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
  timetableType?: string[];
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseDagServiceGetDagsKeyFn({ assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagDisplayNamePrefixPattern, dagIdPattern, dagIdPrefixPattern, dagRunEndDateGt, dagRunEndDateGte, dagRunEndDateLt, dagRunEndDateLte, dagRunStartDateGt, dagRunStartDateGte, dagRunStartDateLt, dagRunStartDateLte, dagRunState, excludeStale, hasAssetSchedule, hasImportErrors, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode, timetableType }, queryKey), queryFn: () => DagService.getDags({ assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagDisplayNamePrefixPattern, dagIdPattern, dagIdPrefixPattern, dagRunEndDateGt, dagRunEndDateGte, dagRunEndDateLt, dagRunEndDateLte, dagRunStartDateGt, dagRunStartDateGte, dagRunStartDateLt, dagRunStartDateLte, dagRunState, excludeStale, hasAssetSchedule, hasImportErrors, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode, timetableType }) as TData, ...options });
/**
* Get Dag
* Get basic information about a Dag.
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
* Get details of Dag.
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
* Get all Dag tags.
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `name`
* @param data.tagNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``tag_name_prefix_pattern`` parameter when possible.
* @param data.tagNamePrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @returns DAGTagCollectionResponse Successful Response
* @throws ApiError
*/
export const useDagServiceGetDagTagsSuspense = <TData = Common.DagServiceGetDagTagsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ limit, offset, orderBy, tagNamePattern, tagNamePrefixPattern }: {
  limit?: number;
  offset?: number;
  orderBy?: string[];
  tagNamePattern?: string;
  tagNamePrefixPattern?: string;
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseDagServiceGetDagTagsKeyFn({ limit, offset, orderBy, tagNamePattern, tagNamePrefixPattern }, queryKey), queryFn: () => DagService.getDagTags({ limit, offset, orderBy, tagNamePattern, tagNamePrefixPattern }) as TData, ...options });
/**
* Get Dags
* Get Dags with recent DagRun.
* @param data The data for the request.
* @param data.dagRunsLimit
* @param data.limit
* @param data.offset
* @param data.tags
* @param data.tagsMatchMode
* @param data.owners
* @param data.dagIds
* @param data.dagIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``dag_id_prefix_pattern`` parameter when possible.
* @param data.dagIdPrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @param data.dagDisplayNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``dag_display_name_prefix_pattern`` parameter when possible.
* @param data.dagDisplayNamePrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
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
export const useDagServiceGetDagsUiSuspense = <TData = Common.DagServiceGetDagsUiDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagDisplayNamePrefixPattern, dagIdPattern, dagIdPrefixPattern, dagIds, dagRunsLimit, excludeStale, hasAssetSchedule, hasImportErrors, hasPendingActions, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode }: {
  assetDependency?: string;
  bundleName?: string;
  bundleVersion?: string;
  dagDisplayNamePattern?: string;
  dagDisplayNamePrefixPattern?: string;
  dagIdPattern?: string;
  dagIdPrefixPattern?: string;
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
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseDagServiceGetDagsUiKeyFn({ assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagDisplayNamePrefixPattern, dagIdPattern, dagIdPrefixPattern, dagIds, dagRunsLimit, excludeStale, hasAssetSchedule, hasImportErrors, hasPendingActions, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode }, queryKey), queryFn: () => DagService.getDagsUi({ assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagDisplayNamePrefixPattern, dagIdPattern, dagIdPrefixPattern, dagIds, dagRunsLimit, excludeStale, hasAssetSchedule, hasImportErrors, hasPendingActions, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode }) as TData, ...options });
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
* @param data.dagIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``dag_id_prefix_pattern`` parameter when possible.
* @param data.taskIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``task_id_prefix_pattern`` parameter when possible.
* @param data.runIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``run_id_prefix_pattern`` parameter when possible.
* @param data.ownerPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``owner_prefix_pattern`` parameter when possible.
* @param data.eventPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``event_prefix_pattern`` parameter when possible.
* @param data.dagIdPrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @param data.taskIdPrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @param data.runIdPrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @param data.ownerPrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @param data.eventPrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @returns EventLogCollectionResponse Successful Response
* @throws ApiError
*/
export const useEventLogServiceGetEventLogsSuspense = <TData = Common.EventLogServiceGetEventLogsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ after, before, dagId, dagIdPattern, dagIdPrefixPattern, event, eventPattern, eventPrefixPattern, excludedEvents, includedEvents, limit, mapIndex, offset, orderBy, owner, ownerPattern, ownerPrefixPattern, runId, runIdPattern, runIdPrefixPattern, taskId, taskIdPattern, taskIdPrefixPattern, tryNumber }: {
  after?: string;
  before?: string;
  dagId?: string;
  dagIdPattern?: string;
  dagIdPrefixPattern?: string;
  event?: string;
  eventPattern?: string;
  eventPrefixPattern?: string;
  excludedEvents?: string[];
  includedEvents?: string[];
  limit?: number;
  mapIndex?: number;
  offset?: number;
  orderBy?: string[];
  owner?: string;
  ownerPattern?: string;
  ownerPrefixPattern?: string;
  runId?: string;
  runIdPattern?: string;
  runIdPrefixPattern?: string;
  taskId?: string;
  taskIdPattern?: string;
  taskIdPrefixPattern?: string;
  tryNumber?: number;
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseEventLogServiceGetEventLogsKeyFn({ after, before, dagId, dagIdPattern, dagIdPrefixPattern, event, eventPattern, eventPrefixPattern, excludedEvents, includedEvents, limit, mapIndex, offset, orderBy, owner, ownerPattern, ownerPrefixPattern, runId, runIdPattern, runIdPrefixPattern, taskId, taskIdPattern, taskIdPrefixPattern, tryNumber }, queryKey), queryFn: () => EventLogService.getEventLogs({ after, before, dagId, dagIdPattern, dagIdPrefixPattern, event, eventPattern, eventPrefixPattern, excludedEvents, includedEvents, limit, mapIndex, offset, orderBy, owner, ownerPattern, ownerPrefixPattern, runId, runIdPattern, runIdPrefixPattern, taskId, taskIdPattern, taskIdPrefixPattern, tryNumber }) as TData, ...options });
/**
* Get Extra Links
* Get extra links for task instance.
* @param data The data for the request.
* @param data.dagId
* @param data.dagRunId
* @param data.taskId
* @param data.mapIndex
* @param data.tryNumber
* @returns ExtraLinkCollectionResponse Successful Response
* @throws ApiError
*/
export const useExtraLinksServiceGetExtraLinksSuspense = <TData = Common.ExtraLinksServiceGetExtraLinksDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, dagRunId, mapIndex, taskId, tryNumber }: {
  dagId: string;
  dagRunId: string;
  mapIndex?: number;
  taskId: string;
  tryNumber?: number;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseExtraLinksServiceGetExtraLinksKeyFn({ dagId, dagRunId, mapIndex, taskId, tryNumber }, queryKey), queryFn: () => ExtraLinksService.getExtraLinks({ dagId, dagRunId, mapIndex, taskId, tryNumber }) as TData, ...options });
/**
* Get Extra Links
* Get extra links for task instance.
* @param data The data for the request.
* @param data.dagId
* @param data.dagRunId
* @param data.taskId
* @param data.mapIndex
* @param data.tryNumber
* @returns ExtraLinkCollectionResponse Successful Response
* @throws ApiError
*/
export const useTaskInstanceServiceGetExtraLinksSuspense = <TData = Common.TaskInstanceServiceGetExtraLinksDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, dagRunId, mapIndex, taskId, tryNumber }: {
  dagId: string;
  dagRunId: string;
  mapIndex?: number;
  taskId: string;
  tryNumber?: number;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseTaskInstanceServiceGetExtraLinksKeyFn({ dagId, dagRunId, mapIndex, taskId, tryNumber }, queryKey), queryFn: () => TaskInstanceService.getExtraLinks({ dagId, dagRunId, mapIndex, taskId, tryNumber }) as TData, ...options });
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
* @param data.poolNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``pool_name_prefix_pattern`` parameter when possible.
* @param data.poolNamePrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @param data.queue
* @param data.queueNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``queue_name_prefix_pattern`` parameter when possible.
* @param data.queueNamePrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @param data.executor
* @param data.versionNumber
* @param data.tryNumber
* @param data.operator
* @param data.operatorNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``operator_name_prefix_pattern`` parameter when possible.
* @param data.operatorNamePrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @param data.mapIndex
* @param data.limit
* @param data.offset
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `id, state, duration, start_date, end_date, map_index, try_number, logical_date, run_after, data_interval_start, data_interval_end, rendered_map_index, operator, run_after, logical_date, data_interval_start, data_interval_end`
* @returns TaskInstanceCollectionResponse Successful Response
* @throws ApiError
*/
export const useTaskInstanceServiceGetMappedTaskInstancesSuspense = <TData = Common.TaskInstanceServiceGetMappedTaskInstancesDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, offset, operator, operatorNamePattern, operatorNamePrefixPattern, orderBy, pool, poolNamePattern, poolNamePrefixPattern, queue, queueNamePattern, queueNamePrefixPattern, runAfterGt, runAfterGte, runAfterLt, runAfterLte, startDateGt, startDateGte, startDateLt, startDateLte, state, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }: {
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
  operatorNamePrefixPattern?: string;
  orderBy?: string[];
  pool?: string[];
  poolNamePattern?: string;
  poolNamePrefixPattern?: string;
  queue?: string[];
  queueNamePattern?: string;
  queueNamePrefixPattern?: string;
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
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstancesKeyFn({ dagId, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, offset, operator, operatorNamePattern, operatorNamePrefixPattern, orderBy, pool, poolNamePattern, poolNamePrefixPattern, queue, queueNamePattern, queueNamePrefixPattern, runAfterGt, runAfterGte, runAfterLt, runAfterLte, startDateGt, startDateGte, startDateLt, startDateLte, state, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }, queryKey), queryFn: () => TaskInstanceService.getMappedTaskInstances({ dagId, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, offset, operator, operatorNamePattern, operatorNamePrefixPattern, orderBy, pool, poolNamePattern, poolNamePrefixPattern, queue, queueNamePattern, queueNamePrefixPattern, runAfterGt, runAfterGte, runAfterLt, runAfterLte, startDateGt, startDateGte, startDateLt, startDateLte, state, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }) as TData, ...options });
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
* This endpoint allows specifying `~` as the dag_id, dag_run_id
* to retrieve task instances for all Dags and Dag runs.
*
* Supports two pagination modes:
*
* **Offset (default):** use `limit` and `offset` query parameters. Returns `total_entries`.
*
* **Cursor:** pass `cursor` (empty string for the first page, then `next_cursor` from the response).
* When `cursor` is provided, `offset` is ignored and `total_entries` is not returned.
* ``next_cursor`` is ``null`` when there are no more pages; ``previous_cursor`` is ``null``
* on the first page.
* @param data The data for the request.
* @param data.dagId
* @param data.dagRunId
* @param data.cursor Cursor for keyset-based pagination. Pass an empty string for the first page, then use ``next_cursor`` from the response. When ``cursor`` is provided, ``offset`` is ignored.
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
* @param data.taskDisplayNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``task_display_name_prefix_pattern`` parameter when possible.
* @param data.taskDisplayNamePrefixPattern Prefix match on task display name: optional ``_task_display_property_value`` else ``task_id`` (same as ``coalesce``). Case-sensitive. Index-friendly alternative to ``task_display_name_pattern``. On large databases, combine with ``dag_id_prefix_pattern`` (or a specific Dag in the path) so ``(dag_id, task_id, ...)`` indexes apply. Use ``|`` for OR. Use ``~`` to match all. Trailing non-alphanumeric characters in the term are stripped before matching so the range scan stays index-compatible under locale-aware collations.
* @param data.taskGroupId Filter by exact task group ID. Returns all tasks within the specified task group.
* @param data.dagIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``dag_id_prefix_pattern`` parameter when possible.
* @param data.dagIdPrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @param data.runIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``run_id_prefix_pattern`` parameter when possible.
* @param data.runIdPrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @param data.state
* @param data.pool
* @param data.poolNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``pool_name_prefix_pattern`` parameter when possible.
* @param data.poolNamePrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @param data.queue
* @param data.queueNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``queue_name_prefix_pattern`` parameter when possible.
* @param data.queueNamePrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @param data.executor
* @param data.versionNumber
* @param data.tryNumber
* @param data.operator
* @param data.operatorNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``operator_name_prefix_pattern`` parameter when possible.
* @param data.operatorNamePrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @param data.mapIndex
* @param data.limit
* @param data.offset
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `id, state, duration, start_date, end_date, map_index, try_number, logical_date, run_after, data_interval_start, data_interval_end, rendered_map_index, operator, logical_date, run_after, data_interval_start, data_interval_end`
* @returns TaskInstanceCollectionResponse Successful Response
* @throws ApiError
*/
export const useTaskInstanceServiceGetTaskInstancesSuspense = <TData = Common.TaskInstanceServiceGetTaskInstancesDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ cursor, dagId, dagIdPattern, dagIdPrefixPattern, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, offset, operator, operatorNamePattern, operatorNamePrefixPattern, orderBy, pool, poolNamePattern, poolNamePrefixPattern, queue, queueNamePattern, queueNamePrefixPattern, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, runIdPrefixPattern, startDateGt, startDateGte, startDateLt, startDateLte, state, taskDisplayNamePattern, taskDisplayNamePrefixPattern, taskGroupId, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }: {
  cursor?: string;
  dagId: string;
  dagIdPattern?: string;
  dagIdPrefixPattern?: string;
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
  operatorNamePrefixPattern?: string;
  orderBy?: string[];
  pool?: string[];
  poolNamePattern?: string;
  poolNamePrefixPattern?: string;
  queue?: string[];
  queueNamePattern?: string;
  queueNamePrefixPattern?: string;
  runAfterGt?: string;
  runAfterGte?: string;
  runAfterLt?: string;
  runAfterLte?: string;
  runIdPattern?: string;
  runIdPrefixPattern?: string;
  startDateGt?: string;
  startDateGte?: string;
  startDateLt?: string;
  startDateLte?: string;
  state?: string[];
  taskDisplayNamePattern?: string;
  taskDisplayNamePrefixPattern?: string;
  taskGroupId?: string;
  taskId?: string;
  tryNumber?: number[];
  updatedAtGt?: string;
  updatedAtGte?: string;
  updatedAtLt?: string;
  updatedAtLte?: string;
  versionNumber?: number[];
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseTaskInstanceServiceGetTaskInstancesKeyFn({ cursor, dagId, dagIdPattern, dagIdPrefixPattern, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, offset, operator, operatorNamePattern, operatorNamePrefixPattern, orderBy, pool, poolNamePattern, poolNamePrefixPattern, queue, queueNamePattern, queueNamePrefixPattern, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, runIdPrefixPattern, startDateGt, startDateGte, startDateLt, startDateLte, state, taskDisplayNamePattern, taskDisplayNamePrefixPattern, taskGroupId, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }, queryKey), queryFn: () => TaskInstanceService.getTaskInstances({ cursor, dagId, dagIdPattern, dagIdPrefixPattern, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, offset, operator, operatorNamePattern, operatorNamePrefixPattern, orderBy, pool, poolNamePattern, poolNamePrefixPattern, queue, queueNamePattern, queueNamePrefixPattern, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, runIdPrefixPattern, startDateGt, startDateGte, startDateLt, startDateLte, state, taskDisplayNamePattern, taskDisplayNamePrefixPattern, taskGroupId, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }) as TData, ...options });
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
export const useTaskInstanceServiceGetHitlDetailSuspense = <TData = Common.TaskInstanceServiceGetHitlDetailDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, dagRunId, mapIndex, taskId }: {
  dagId: string;
  dagRunId: string;
  mapIndex: number;
  taskId: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseTaskInstanceServiceGetHitlDetailKeyFn({ dagId, dagRunId, mapIndex, taskId }, queryKey), queryFn: () => TaskInstanceService.getHitlDetail({ dagId, dagRunId, mapIndex, taskId }) as TData, ...options });
/**
* Get Hitl Detail Try Detail
* Get a Human-in-the-loop detail of a specific task instance.
* @param data The data for the request.
* @param data.dagId
* @param data.dagRunId
* @param data.taskId
* @param data.mapIndex
* @param data.tryNumber
* @returns HITLDetailHistory Successful Response
* @throws ApiError
*/
export const useTaskInstanceServiceGetHitlDetailTryDetailSuspense = <TData = Common.TaskInstanceServiceGetHitlDetailTryDetailDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, dagRunId, mapIndex, taskId, tryNumber }: {
  dagId: string;
  dagRunId: string;
  mapIndex: number;
  taskId: string;
  tryNumber: number;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseTaskInstanceServiceGetHitlDetailTryDetailKeyFn({ dagId, dagRunId, mapIndex, taskId, tryNumber }, queryKey), queryFn: () => TaskInstanceService.getHitlDetailTryDetail({ dagId, dagRunId, mapIndex, taskId, tryNumber }) as TData, ...options });
/**
* Get Hitl Details
* Get Human-in-the-loop details.
* @param data The data for the request.
* @param data.dagId
* @param data.dagRunId
* @param data.limit
* @param data.offset
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `ti_id, subject, responded_at, created_at, responded_by_user_id, responded_by_user_name, dag_id, run_id, task_display_name, run_after, rendered_map_index, task_instance_operator, task_instance_state`
* @param data.dagIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``dag_id_prefix_pattern`` parameter when possible.
* @param data.dagIdPrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @param data.taskId
* @param data.taskIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``task_id_prefix_pattern`` parameter when possible.
* @param data.taskIdPrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @param data.mapIndex
* @param data.state
* @param data.responseReceived
* @param data.respondedByUserId
* @param data.respondedByUserName
* @param data.subjectSearch SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``subject_search`` parameter when possible.
* @param data.bodySearch SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``body_search`` parameter when possible.
* @param data.createdAtGte
* @param data.createdAtGt
* @param data.createdAtLte
* @param data.createdAtLt
* @returns HITLDetailCollection Successful Response
* @throws ApiError
*/
export const useTaskInstanceServiceGetHitlDetailsSuspense = <TData = Common.TaskInstanceServiceGetHitlDetailsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ bodySearch, createdAtGt, createdAtGte, createdAtLt, createdAtLte, dagId, dagIdPattern, dagIdPrefixPattern, dagRunId, limit, mapIndex, offset, orderBy, respondedByUserId, respondedByUserName, responseReceived, state, subjectSearch, taskId, taskIdPattern, taskIdPrefixPattern }: {
  bodySearch?: string;
  createdAtGt?: string;
  createdAtGte?: string;
  createdAtLt?: string;
  createdAtLte?: string;
  dagId: string;
  dagIdPattern?: string;
  dagIdPrefixPattern?: string;
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
  taskIdPrefixPattern?: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseTaskInstanceServiceGetHitlDetailsKeyFn({ bodySearch, createdAtGt, createdAtGte, createdAtLt, createdAtLte, dagId, dagIdPattern, dagIdPrefixPattern, dagRunId, limit, mapIndex, offset, orderBy, respondedByUserId, respondedByUserName, responseReceived, state, subjectSearch, taskId, taskIdPattern, taskIdPrefixPattern }, queryKey), queryFn: () => TaskInstanceService.getHitlDetails({ bodySearch, createdAtGt, createdAtGte, createdAtLt, createdAtLte, dagId, dagIdPattern, dagIdPrefixPattern, dagRunId, limit, mapIndex, offset, orderBy, respondedByUserId, respondedByUserName, responseReceived, state, subjectSearch, taskId, taskIdPattern, taskIdPrefixPattern }) as TData, ...options });
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
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `id, timestamp, filename, bundle_name, stacktrace, import_error_id`
* @param data.filenamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``filename_prefix_pattern`` parameter when possible.
* @param data.filenamePrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @returns ImportErrorCollectionResponse Successful Response
* @throws ApiError
*/
export const useImportErrorServiceGetImportErrorsSuspense = <TData = Common.ImportErrorServiceGetImportErrorsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ filenamePattern, filenamePrefixPattern, limit, offset, orderBy }: {
  filenamePattern?: string;
  filenamePrefixPattern?: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseImportErrorServiceGetImportErrorsKeyFn({ filenamePattern, filenamePrefixPattern, limit, offset, orderBy }, queryKey), queryFn: () => ImportErrorService.getImportErrors({ filenamePattern, filenamePrefixPattern, limit, offset, orderBy }) as TData, ...options });
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
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `id, pool, name`
* @param data.poolNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``pool_name_prefix_pattern`` parameter when possible.
* @param data.poolNamePrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @returns PoolCollectionResponse Successful Response
* @throws ApiError
*/
export const usePoolServiceGetPoolsSuspense = <TData = Common.PoolServiceGetPoolsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ limit, offset, orderBy, poolNamePattern, poolNamePrefixPattern }: {
  limit?: number;
  offset?: number;
  orderBy?: string[];
  poolNamePattern?: string;
  poolNamePrefixPattern?: string;
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UsePoolServiceGetPoolsKeyFn({ limit, offset, orderBy, poolNamePattern, poolNamePrefixPattern }, queryKey), queryFn: () => PoolService.getPools({ limit, offset, orderBy, poolNamePattern, poolNamePrefixPattern }) as TData, ...options });
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
* This endpoint allows specifying `~` as the dag_id, dag_run_id, task_id to retrieve XCom entries for all Dags.
* @param data The data for the request.
* @param data.dagId
* @param data.dagRunId
* @param data.taskId
* @param data.xcomKey
* @param data.mapIndex
* @param data.limit
* @param data.offset
* @param data.xcomKeyPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``xcom_key_prefix_pattern`` parameter when possible.
* @param data.xcomKeyPrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @param data.dagDisplayNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``dag_display_name_prefix_pattern`` parameter when possible.
* @param data.dagDisplayNamePrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @param data.runIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``run_id_prefix_pattern`` parameter when possible.
* @param data.runIdPrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @param data.taskIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``task_id_prefix_pattern`` parameter when possible.
* @param data.taskIdPrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @param data.mapIndexFilter
* @param data.logicalDateGte
* @param data.logicalDateGt
* @param data.logicalDateLte
* @param data.logicalDateLt
* @param data.runAfterGte
* @param data.runAfterGt
* @param data.runAfterLte
* @param data.runAfterLt
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `key, dag_id, run_id, task_id, map_index, timestamp, run_after`
* @returns XComCollectionResponse Successful Response
* @throws ApiError
*/
export const useXcomServiceGetXcomEntriesSuspense = <TData = Common.XcomServiceGetXcomEntriesDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagDisplayNamePattern, dagDisplayNamePrefixPattern, dagId, dagRunId, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, mapIndexFilter, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, runIdPrefixPattern, taskId, taskIdPattern, taskIdPrefixPattern, xcomKey, xcomKeyPattern, xcomKeyPrefixPattern }: {
  dagDisplayNamePattern?: string;
  dagDisplayNamePrefixPattern?: string;
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
  orderBy?: string[];
  runAfterGt?: string;
  runAfterGte?: string;
  runAfterLt?: string;
  runAfterLte?: string;
  runIdPattern?: string;
  runIdPrefixPattern?: string;
  taskId: string;
  taskIdPattern?: string;
  taskIdPrefixPattern?: string;
  xcomKey?: string;
  xcomKeyPattern?: string;
  xcomKeyPrefixPattern?: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseXcomServiceGetXcomEntriesKeyFn({ dagDisplayNamePattern, dagDisplayNamePrefixPattern, dagId, dagRunId, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, mapIndexFilter, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, runIdPrefixPattern, taskId, taskIdPattern, taskIdPrefixPattern, xcomKey, xcomKeyPattern, xcomKeyPrefixPattern }, queryKey), queryFn: () => XcomService.getXcomEntries({ dagDisplayNamePattern, dagDisplayNamePrefixPattern, dagId, dagRunId, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, mapIndexFilter, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, runIdPrefixPattern, taskId, taskIdPattern, taskIdPrefixPattern, xcomKey, xcomKeyPattern, xcomKeyPrefixPattern }) as TData, ...options });
/**
* Get Tasks
* Get tasks for Dag.
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
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `key, id, _val, description, is_encrypted, team_name`
* @param data.variableKeyPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``variable_key_prefix_pattern`` parameter when possible.
* @param data.variableKeyPrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @returns VariableCollectionResponse Successful Response
* @throws ApiError
*/
export const useVariableServiceGetVariablesSuspense = <TData = Common.VariableServiceGetVariablesDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ limit, offset, orderBy, variableKeyPattern, variableKeyPrefixPattern }: {
  limit?: number;
  offset?: number;
  orderBy?: string[];
  variableKeyPattern?: string;
  variableKeyPrefixPattern?: string;
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseVariableServiceGetVariablesKeyFn({ limit, offset, orderBy, variableKeyPattern, variableKeyPrefixPattern }, queryKey), queryFn: () => VariableService.getVariables({ limit, offset, orderBy, variableKeyPattern, variableKeyPrefixPattern }) as TData, ...options });
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
* Get all Dag Versions.
*
* This endpoint allows specifying `~` as the dag_id to retrieve Dag Versions for all Dags.
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
* @returns unknown Successful Response
* @throws ApiError
*/
export const useLoginServiceLogoutSuspense = <TData = Common.LoginServiceLogoutDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>(queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseLoginServiceLogoutKeyFn(queryKey), queryFn: () => LoginService.logout() as TData, ...options });
/**
* Get Auth Menus
* @returns MenuItemCollectionResponse Successful Response
* @throws ApiError
*/
export const useAuthLinksServiceGetAuthMenusSuspense = <TData = Common.AuthLinksServiceGetAuthMenusDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>(queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseAuthLinksServiceGetAuthMenusKeyFn(queryKey), queryFn: () => AuthLinksService.getAuthMenus() as TData, ...options });
/**
* Get Current User Info
* Convienently get the current authenticated user information.
* @returns AuthenticatedMeResponse Successful Response
* @throws ApiError
*/
export const useAuthLinksServiceGetCurrentUserInfoSuspense = <TData = Common.AuthLinksServiceGetCurrentUserInfoDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>(queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseAuthLinksServiceGetCurrentUserInfoKeyFn(queryKey), queryFn: () => AuthLinksService.getCurrentUserInfo() as TData, ...options });
/**
* Get Partitioned Dag Runs
* Return PartitionedDagRuns. Filter by dag_id and/or has_created_dag_run_id.
* @param data The data for the request.
* @param data.dagId
* @param data.hasCreatedDagRunId
* @returns PartitionedDagRunCollectionResponse Successful Response
* @throws ApiError
*/
export const usePartitionedDagRunServiceGetPartitionedDagRunsSuspense = <TData = Common.PartitionedDagRunServiceGetPartitionedDagRunsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, hasCreatedDagRunId }: {
  dagId?: string;
  hasCreatedDagRunId?: boolean;
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UsePartitionedDagRunServiceGetPartitionedDagRunsKeyFn({ dagId, hasCreatedDagRunId }, queryKey), queryFn: () => PartitionedDagRunService.getPartitionedDagRuns({ dagId, hasCreatedDagRunId }) as TData, ...options });
/**
* Get Pending Partitioned Dag Run
* Return full details for pending PartitionedDagRun.
* @param data The data for the request.
* @param data.dagId
* @param data.partitionKey
* @returns PartitionedDagRunDetailResponse Successful Response
* @throws ApiError
*/
export const usePartitionedDagRunServiceGetPendingPartitionedDagRunSuspense = <TData = Common.PartitionedDagRunServiceGetPendingPartitionedDagRunDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, partitionKey }: {
  dagId: string;
  partitionKey: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UsePartitionedDagRunServiceGetPendingPartitionedDagRunKeyFn({ dagId, partitionKey }, queryKey), queryFn: () => PartitionedDagRunService.getPendingPartitionedDagRun({ dagId, partitionKey }) as TData, ...options });
/**
* Get Dependencies
* Dependencies graph.
* @param data The data for the request.
* @param data.nodeId
* @param data.dependencyType
* @returns BaseGraphResponse Successful Response
* @throws ApiError
*/
export const useDependenciesServiceGetDependenciesSuspense = <TData = Common.DependenciesServiceGetDependenciesDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dependencyType, nodeId }: {
  dependencyType?: "scheduling" | "data";
  nodeId?: string;
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseDependenciesServiceGetDependenciesKeyFn({ dependencyType, nodeId }, queryKey), queryFn: () => DependenciesService.getDependencies({ dependencyType, nodeId }) as TData, ...options });
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
* Return basic Dag stats with counts of Dags in various states.
* @returns DashboardDagStatsResponse Successful Response
* @throws ApiError
*/
export const useDashboardServiceDagStatsSuspense = <TData = Common.DashboardServiceDagStatsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>(queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseDashboardServiceDagStatsKeyFn(queryKey), queryFn: () => DashboardService.dagStats() as TData, ...options });
/**
* Get Deadlines
* Get deadlines for a Dag run.
*
* This endpoint allows specifying `~` as the dag_id and dag_run_id to retrieve Deadlines for all
* Dags and Dag runs.
* @param data The data for the request.
* @param data.dagId
* @param data.dagRunId
* @param data.limit
* @param data.offset
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `id, deadline_time, created_at, last_updated_at, missed, dag_id, dag_run_id, alert_name`
* @param data.missed
* @param data.deadlineTimeGte
* @param data.deadlineTimeGt
* @param data.deadlineTimeLte
* @param data.deadlineTimeLt
* @param data.lastUpdatedAtGte
* @param data.lastUpdatedAtGt
* @param data.lastUpdatedAtLte
* @param data.lastUpdatedAtLt
* @returns DeadlineCollectionResponse Successful Response
* @throws ApiError
*/
export const useDeadlinesServiceGetDeadlinesSuspense = <TData = Common.DeadlinesServiceGetDeadlinesDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, dagRunId, deadlineTimeGt, deadlineTimeGte, deadlineTimeLt, deadlineTimeLte, lastUpdatedAtGt, lastUpdatedAtGte, lastUpdatedAtLt, lastUpdatedAtLte, limit, missed, offset, orderBy }: {
  dagId: string;
  dagRunId: string;
  deadlineTimeGt?: string;
  deadlineTimeGte?: string;
  deadlineTimeLt?: string;
  deadlineTimeLte?: string;
  lastUpdatedAtGt?: string;
  lastUpdatedAtGte?: string;
  lastUpdatedAtLt?: string;
  lastUpdatedAtLte?: string;
  limit?: number;
  missed?: boolean;
  offset?: number;
  orderBy?: string[];
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseDeadlinesServiceGetDeadlinesKeyFn({ dagId, dagRunId, deadlineTimeGt, deadlineTimeGte, deadlineTimeLt, deadlineTimeLte, lastUpdatedAtGt, lastUpdatedAtGte, lastUpdatedAtLt, lastUpdatedAtLte, limit, missed, offset, orderBy }, queryKey), queryFn: () => DeadlinesService.getDeadlines({ dagId, dagRunId, deadlineTimeGt, deadlineTimeGte, deadlineTimeLt, deadlineTimeLte, lastUpdatedAtGt, lastUpdatedAtGte, lastUpdatedAtLt, lastUpdatedAtLte, limit, missed, offset, orderBy }) as TData, ...options });
/**
* Get Dag Deadline Alerts
* Get all deadline alerts defined on a Dag.
* @param data The data for the request.
* @param data.dagId
* @param data.limit
* @param data.offset
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `id, created_at, name, interval`
* @returns DeadlineAlertCollectionResponse Successful Response
* @throws ApiError
*/
export const useDeadlinesServiceGetDagDeadlineAlertsSuspense = <TData = Common.DeadlinesServiceGetDagDeadlineAlertsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, limit, offset, orderBy }: {
  dagId: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseDeadlinesServiceGetDagDeadlineAlertsKeyFn({ dagId, limit, offset, orderBy }, queryKey), queryFn: () => DeadlinesService.getDagDeadlineAlerts({ dagId, limit, offset, orderBy }) as TData, ...options });
/**
* Structure Data
* Get Structure Data.
* @param data The data for the request.
* @param data.dagId
* @param data.includeUpstream
* @param data.includeDownstream
* @param data.depth
* @param data.root
* @param data.externalDependencies
* @param data.versionNumber
* @returns StructureDataResponse Successful Response
* @throws ApiError
*/
export const useStructureServiceStructureDataSuspense = <TData = Common.StructureServiceStructureDataDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, depth, externalDependencies, includeDownstream, includeUpstream, root, versionNumber }: {
  dagId: string;
  depth?: number;
  externalDependencies?: boolean;
  includeDownstream?: boolean;
  includeUpstream?: boolean;
  root?: string;
  versionNumber?: number;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseStructureServiceStructureDataKeyFn({ dagId, depth, externalDependencies, includeDownstream, includeUpstream, root, versionNumber }, queryKey), queryFn: () => StructureService.structureData({ dagId, depth, externalDependencies, includeDownstream, includeUpstream, root, versionNumber }) as TData, ...options });
/**
* Get Dag Structure
* Return dag structure for grid view.
* @param data The data for the request.
* @param data.dagId
* @param data.includeUpstream
* @param data.includeDownstream
* @param data.depth
* @param data.root
* @param data.offset
* @param data.limit
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `run_after, logical_date, start_date, end_date`
* @param data.runAfterGte
* @param data.runAfterGt
* @param data.runAfterLte
* @param data.runAfterLt
* @param data.runType
* @param data.state
* @param data.triggeringUser SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``triggering_user`` parameter when possible.
* @param data.triggeringUserPrefix Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @returns GridNodeResponse Successful Response
* @throws ApiError
*/
export const useGridServiceGetDagStructureSuspense = <TData = Common.GridServiceGetDagStructureDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, depth, includeDownstream, includeUpstream, limit, offset, orderBy, root, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runType, state, triggeringUser, triggeringUserPrefix }: {
  dagId: string;
  depth?: number;
  includeDownstream?: boolean;
  includeUpstream?: boolean;
  limit?: number;
  offset?: number;
  orderBy?: string[];
  root?: string;
  runAfterGt?: string;
  runAfterGte?: string;
  runAfterLt?: string;
  runAfterLte?: string;
  runType?: string[];
  state?: string[];
  triggeringUser?: string;
  triggeringUserPrefix?: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseGridServiceGetDagStructureKeyFn({ dagId, depth, includeDownstream, includeUpstream, limit, offset, orderBy, root, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runType, state, triggeringUser, triggeringUserPrefix }, queryKey), queryFn: () => GridService.getDagStructure({ dagId, depth, includeDownstream, includeUpstream, limit, offset, orderBy, root, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runType, state, triggeringUser, triggeringUserPrefix }) as TData, ...options });
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
* @param data.triggeringUser SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``triggering_user`` parameter when possible.
* @param data.triggeringUserPrefix Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @returns GridRunsResponse Successful Response
* @throws ApiError
*/
export const useGridServiceGetGridRunsSuspense = <TData = Common.GridServiceGetGridRunsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, limit, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runType, state, triggeringUser, triggeringUserPrefix }: {
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
  triggeringUserPrefix?: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseGridServiceGetGridRunsKeyFn({ dagId, limit, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runType, state, triggeringUser, triggeringUserPrefix }, queryKey), queryFn: () => GridService.getGridRuns({ dagId, limit, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runType, state, triggeringUser, triggeringUserPrefix }) as TData, ...options });
/**
* Get Grid Ti Summaries Stream
* Stream TI summaries for multiple Dag runs as NDJSON (one JSON line per run).
*
* Each line is a serialized ``GridTISummaries`` object emitted as soon as that
* run's task instances have been processed, so the client can render columns
* progressively without waiting for all runs to complete.
*
* The serialized Dag structure is served from the app-wide ``DBDagBag`` cache
* (keyed by ``dag_version_id``), which avoids repeated deserialization across
* runs of the same version *and* across requests.
* @param data The data for the request.
* @param data.dagId
* @param data.runIds
* @returns string NDJSON stream — one ``GridTISummaries`` JSON object per line, one per Dag run
* @throws ApiError
*/
export const useGridServiceGetGridTiSummariesStreamSuspense = <TData = Common.GridServiceGetGridTiSummariesStreamDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, runIds }: {
  dagId: string;
  runIds?: string[];
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseGridServiceGetGridTiSummariesStreamKeyFn({ dagId, runIds }, queryKey), queryFn: () => GridService.getGridTiSummariesStream({ dagId, runIds }) as TData, ...options });
/**
* Get Gantt Data
* Get all task instance tries for Gantt chart.
* @param data The data for the request.
* @param data.dagId
* @param data.runId
* @returns GanttResponse Successful Response
* @throws ApiError
*/
export const useGanttServiceGetGanttDataSuspense = <TData = Common.GanttServiceGetGanttDataDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, runId }: {
  dagId: string;
  runId: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseGanttServiceGetGanttDataKeyFn({ dagId, runId }, queryKey), queryFn: () => GanttService.getGanttData({ dagId, runId }) as TData, ...options });
/**
* Get Calendar
* Get calendar data for a Dag including historical and planned Dag runs.
* @param data The data for the request.
* @param data.dagId
* @param data.granularity
* @param data.logicalDateGte
* @param data.logicalDateGt
* @param data.logicalDateLte
* @param data.logicalDateLt
* @param data.partitionDateGte
* @param data.partitionDateGt
* @param data.partitionDateLte
* @param data.partitionDateLt
* @returns CalendarTimeRangeCollectionResponse Successful Response
* @throws ApiError
*/
export const useCalendarServiceGetCalendarSuspense = <TData = Common.CalendarServiceGetCalendarDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ dagId, granularity, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, partitionDateGt, partitionDateGte, partitionDateLt, partitionDateLte }: {
  dagId: string;
  granularity?: "hourly" | "daily";
  logicalDateGt?: string;
  logicalDateGte?: string;
  logicalDateLt?: string;
  logicalDateLte?: string;
  partitionDateGt?: string;
  partitionDateGte?: string;
  partitionDateLt?: string;
  partitionDateLte?: string;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseCalendarServiceGetCalendarKeyFn({ dagId, granularity, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, partitionDateGt, partitionDateGte, partitionDateLt, partitionDateLte }, queryKey), queryFn: () => CalendarService.getCalendar({ dagId, granularity, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, partitionDateGt, partitionDateGte, partitionDateLt, partitionDateLte }) as TData, ...options });
/**
* List Teams
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `name`
* @returns TeamCollectionResponse Successful Response
* @throws ApiError
*/
export const useTeamsServiceListTeamsSuspense = <TData = Common.TeamsServiceListTeamsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ limit, offset, orderBy }: {
  limit?: number;
  offset?: number;
  orderBy?: string[];
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseTeamsServiceListTeamsKeyFn({ limit, offset, orderBy }, queryKey), queryFn: () => TeamsService.listTeams({ limit, offset, orderBy }) as TData, ...options });
