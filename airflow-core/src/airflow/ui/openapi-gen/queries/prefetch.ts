// generated with @7nohe/openapi-react-query-codegen@1.6.2 

import { type QueryClient } from "@tanstack/react-query";
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
export const prefetchUseAssetServiceGetAssets = (queryClient: QueryClient, { dagIds, limit, namePattern, namePrefixPattern, offset, onlyActive, orderBy, uriPattern, uriPrefixPattern }: {
  dagIds?: string[];
  limit?: number;
  namePattern?: string;
  namePrefixPattern?: string;
  offset?: number;
  onlyActive?: boolean;
  orderBy?: string[];
  uriPattern?: string;
  uriPrefixPattern?: string;
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseAssetServiceGetAssetsKeyFn({ dagIds, limit, namePattern, namePrefixPattern, offset, onlyActive, orderBy, uriPattern, uriPrefixPattern }), queryFn: () => AssetService.getAssets({ dagIds, limit, namePattern, namePrefixPattern, offset, onlyActive, orderBy, uriPattern, uriPrefixPattern }) });
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
export const prefetchUseAssetServiceGetAssetAliases = (queryClient: QueryClient, { limit, namePattern, namePrefixPattern, offset, orderBy }: {
  limit?: number;
  namePattern?: string;
  namePrefixPattern?: string;
  offset?: number;
  orderBy?: string[];
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseAssetServiceGetAssetAliasesKeyFn({ limit, namePattern, namePrefixPattern, offset, orderBy }), queryFn: () => AssetService.getAssetAliases({ limit, namePattern, namePrefixPattern, offset, orderBy }) });
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
export const prefetchUseAssetServiceGetAssetEvents = (queryClient: QueryClient, { assetId, limit, namePattern, namePrefixPattern, offset, orderBy, sourceDagId, sourceMapIndex, sourceRunId, sourceTaskId, timestampGt, timestampGte, timestampLt, timestampLte }: {
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
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseAssetServiceGetAssetEventsKeyFn({ assetId, limit, namePattern, namePrefixPattern, offset, orderBy, sourceDagId, sourceMapIndex, sourceRunId, sourceTaskId, timestampGt, timestampGte, timestampLt, timestampLte }), queryFn: () => AssetService.getAssetEvents({ assetId, limit, namePattern, namePrefixPattern, offset, orderBy, sourceDagId, sourceMapIndex, sourceRunId, sourceTaskId, timestampGt, timestampGte, timestampLt, timestampLte }) });
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
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `id`
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
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `id`
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
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `conn_id, conn_type, description, host, port, id, team_name, connection_id`
* @param data.connectionIdPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``connection_id_prefix_pattern`` parameter when possible.
* @param data.connectionIdPrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @returns ConnectionCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseConnectionServiceGetConnections = (queryClient: QueryClient, { connectionIdPattern, connectionIdPrefixPattern, limit, offset, orderBy }: {
  connectionIdPattern?: string;
  connectionIdPrefixPattern?: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseConnectionServiceGetConnectionsKeyFn({ connectionIdPattern, connectionIdPrefixPattern, limit, offset, orderBy }), queryFn: () => ConnectionService.getConnections({ connectionIdPattern, connectionIdPrefixPattern, limit, offset, orderBy }) });
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
export const prefetchUseDagRunServiceGetDagRuns = (queryClient: QueryClient, { bundleVersion, confContains, consumingAssetPattern, cursor, dagId, dagIdPattern, dagIdPrefixPattern, dagVersion, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, offset, orderBy, partitionKeyPattern, partitionKeyPrefixPattern, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, runIdPrefixPattern, runType, startDateGt, startDateGte, startDateLt, startDateLte, state, triggeringUserNamePattern, triggeringUserNamePrefixPattern, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte }: {
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
}) => queryClient.prefetchQuery({ queryKey: Common.UseDagRunServiceGetDagRunsKeyFn({ bundleVersion, confContains, consumingAssetPattern, cursor, dagId, dagIdPattern, dagIdPrefixPattern, dagVersion, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, offset, orderBy, partitionKeyPattern, partitionKeyPrefixPattern, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, runIdPrefixPattern, runType, startDateGt, startDateGte, startDateLt, startDateLte, state, triggeringUserNamePattern, triggeringUserNamePrefixPattern, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte }), queryFn: () => DagRunService.getDagRuns({ bundleVersion, confContains, consumingAssetPattern, cursor, dagId, dagIdPattern, dagIdPrefixPattern, dagVersion, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, offset, orderBy, partitionKeyPattern, partitionKeyPrefixPattern, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, runIdPrefixPattern, runType, startDateGt, startDateGte, startDateLt, startDateLte, state, triggeringUserNamePattern, triggeringUserNamePrefixPattern, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte }) });
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
export const prefetchUseDagRunServiceWaitDagRunUntilFinished = (queryClient: QueryClient, { dagId, dagRunId, interval, result }: {
  dagId: string;
  dagRunId: string;
  interval: number;
  result?: string[];
}) => queryClient.prefetchQuery({ queryKey: Common.UseDagRunServiceWaitDagRunUntilFinishedKeyFn({ dagId, dagRunId, interval, result }), queryFn: () => DagRunService.waitDagRunUntilFinished({ dagId, dagRunId, interval, result }) });
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
export const prefetchUseDagServiceGetDags = (queryClient: QueryClient, { assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagDisplayNamePrefixPattern, dagIdPattern, dagIdPrefixPattern, dagRunEndDateGt, dagRunEndDateGte, dagRunEndDateLt, dagRunEndDateLte, dagRunStartDateGt, dagRunStartDateGte, dagRunStartDateLt, dagRunStartDateLte, dagRunState, excludeStale, hasAssetSchedule, hasImportErrors, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode, timetableType }: {
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
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseDagServiceGetDagsKeyFn({ assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagDisplayNamePrefixPattern, dagIdPattern, dagIdPrefixPattern, dagRunEndDateGt, dagRunEndDateGte, dagRunEndDateLt, dagRunEndDateLte, dagRunStartDateGt, dagRunStartDateGte, dagRunStartDateLt, dagRunStartDateLte, dagRunState, excludeStale, hasAssetSchedule, hasImportErrors, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode, timetableType }), queryFn: () => DagService.getDags({ assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagDisplayNamePrefixPattern, dagIdPattern, dagIdPrefixPattern, dagRunEndDateGt, dagRunEndDateGte, dagRunEndDateLt, dagRunEndDateLte, dagRunStartDateGt, dagRunStartDateGte, dagRunStartDateLt, dagRunStartDateLte, dagRunState, excludeStale, hasAssetSchedule, hasImportErrors, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode, timetableType }) });
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
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `name`
* @param data.tagNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``tag_name_prefix_pattern`` parameter when possible.
* @param data.tagNamePrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @returns DAGTagCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseDagServiceGetDagTags = (queryClient: QueryClient, { limit, offset, orderBy, tagNamePattern, tagNamePrefixPattern }: {
  limit?: number;
  offset?: number;
  orderBy?: string[];
  tagNamePattern?: string;
  tagNamePrefixPattern?: string;
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseDagServiceGetDagTagsKeyFn({ limit, offset, orderBy, tagNamePattern, tagNamePrefixPattern }), queryFn: () => DagService.getDagTags({ limit, offset, orderBy, tagNamePattern, tagNamePrefixPattern }) });
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
export const prefetchUseDagServiceGetDagsUi = (queryClient: QueryClient, { assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagDisplayNamePrefixPattern, dagIdPattern, dagIdPrefixPattern, dagIds, dagRunsLimit, excludeStale, hasAssetSchedule, hasImportErrors, hasPendingActions, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode }: {
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
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseDagServiceGetDagsUiKeyFn({ assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagDisplayNamePrefixPattern, dagIdPattern, dagIdPrefixPattern, dagIds, dagRunsLimit, excludeStale, hasAssetSchedule, hasImportErrors, hasPendingActions, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode }), queryFn: () => DagService.getDagsUi({ assetDependency, bundleName, bundleVersion, dagDisplayNamePattern, dagDisplayNamePrefixPattern, dagIdPattern, dagIdPrefixPattern, dagIds, dagRunsLimit, excludeStale, hasAssetSchedule, hasImportErrors, hasPendingActions, isFavorite, lastDagRunState, limit, offset, orderBy, owners, paused, tags, tagsMatchMode }) });
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
export const prefetchUseEventLogServiceGetEventLogs = (queryClient: QueryClient, { after, before, dagId, dagIdPattern, dagIdPrefixPattern, event, eventPattern, eventPrefixPattern, excludedEvents, includedEvents, limit, mapIndex, offset, orderBy, owner, ownerPattern, ownerPrefixPattern, runId, runIdPattern, runIdPrefixPattern, taskId, taskIdPattern, taskIdPrefixPattern, tryNumber }: {
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
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseEventLogServiceGetEventLogsKeyFn({ after, before, dagId, dagIdPattern, dagIdPrefixPattern, event, eventPattern, eventPrefixPattern, excludedEvents, includedEvents, limit, mapIndex, offset, orderBy, owner, ownerPattern, ownerPrefixPattern, runId, runIdPattern, runIdPrefixPattern, taskId, taskIdPattern, taskIdPrefixPattern, tryNumber }), queryFn: () => EventLogService.getEventLogs({ after, before, dagId, dagIdPattern, dagIdPrefixPattern, event, eventPattern, eventPrefixPattern, excludedEvents, includedEvents, limit, mapIndex, offset, orderBy, owner, ownerPattern, ownerPrefixPattern, runId, runIdPattern, runIdPrefixPattern, taskId, taskIdPattern, taskIdPrefixPattern, tryNumber }) });
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
export const prefetchUseTaskInstanceServiceGetMappedTaskInstances = (queryClient: QueryClient, { dagId, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, offset, operator, operatorNamePattern, operatorNamePrefixPattern, orderBy, pool, poolNamePattern, poolNamePrefixPattern, queue, queueNamePattern, queueNamePrefixPattern, runAfterGt, runAfterGte, runAfterLt, runAfterLte, startDateGt, startDateGte, startDateLt, startDateLte, state, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }: {
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
}) => queryClient.prefetchQuery({ queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstancesKeyFn({ dagId, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, offset, operator, operatorNamePattern, operatorNamePrefixPattern, orderBy, pool, poolNamePattern, poolNamePrefixPattern, queue, queueNamePattern, queueNamePrefixPattern, runAfterGt, runAfterGte, runAfterLt, runAfterLte, startDateGt, startDateGte, startDateLt, startDateLte, state, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }), queryFn: () => TaskInstanceService.getMappedTaskInstances({ dagId, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, offset, operator, operatorNamePattern, operatorNamePrefixPattern, orderBy, pool, poolNamePattern, poolNamePrefixPattern, queue, queueNamePattern, queueNamePrefixPattern, runAfterGt, runAfterGte, runAfterLt, runAfterLte, startDateGt, startDateGte, startDateLt, startDateLte, state, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }) });
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
* This endpoint allows specifying `~` as the dag_id, dag_run_id
* to retrieve task instances for all DAGs and DAG runs.
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
export const prefetchUseTaskInstanceServiceGetTaskInstances = (queryClient: QueryClient, { cursor, dagId, dagIdPattern, dagIdPrefixPattern, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, offset, operator, operatorNamePattern, operatorNamePrefixPattern, orderBy, pool, poolNamePattern, poolNamePrefixPattern, queue, queueNamePattern, queueNamePrefixPattern, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, runIdPrefixPattern, startDateGt, startDateGte, startDateLt, startDateLte, state, taskDisplayNamePattern, taskDisplayNamePrefixPattern, taskGroupId, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }: {
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
}) => queryClient.prefetchQuery({ queryKey: Common.UseTaskInstanceServiceGetTaskInstancesKeyFn({ cursor, dagId, dagIdPattern, dagIdPrefixPattern, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, offset, operator, operatorNamePattern, operatorNamePrefixPattern, orderBy, pool, poolNamePattern, poolNamePrefixPattern, queue, queueNamePattern, queueNamePrefixPattern, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, runIdPrefixPattern, startDateGt, startDateGte, startDateLt, startDateLte, state, taskDisplayNamePattern, taskDisplayNamePrefixPattern, taskGroupId, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }), queryFn: () => TaskInstanceService.getTaskInstances({ cursor, dagId, dagIdPattern, dagIdPrefixPattern, dagRunId, durationGt, durationGte, durationLt, durationLte, endDateGt, endDateGte, endDateLt, endDateLte, executor, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, offset, operator, operatorNamePattern, operatorNamePrefixPattern, orderBy, pool, poolNamePattern, poolNamePrefixPattern, queue, queueNamePattern, queueNamePrefixPattern, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, runIdPrefixPattern, startDateGt, startDateGte, startDateLt, startDateLte, state, taskDisplayNamePattern, taskDisplayNamePrefixPattern, taskGroupId, taskId, tryNumber, updatedAtGt, updatedAtGte, updatedAtLt, updatedAtLte, versionNumber }) });
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
export const prefetchUseTaskInstanceServiceGetHitlDetail = (queryClient: QueryClient, { dagId, dagRunId, mapIndex, taskId }: {
  dagId: string;
  dagRunId: string;
  mapIndex: number;
  taskId: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseTaskInstanceServiceGetHitlDetailKeyFn({ dagId, dagRunId, mapIndex, taskId }), queryFn: () => TaskInstanceService.getHitlDetail({ dagId, dagRunId, mapIndex, taskId }) });
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
export const prefetchUseTaskInstanceServiceGetHitlDetailTryDetail = (queryClient: QueryClient, { dagId, dagRunId, mapIndex, taskId, tryNumber }: {
  dagId: string;
  dagRunId: string;
  mapIndex: number;
  taskId: string;
  tryNumber: number;
}) => queryClient.prefetchQuery({ queryKey: Common.UseTaskInstanceServiceGetHitlDetailTryDetailKeyFn({ dagId, dagRunId, mapIndex, taskId, tryNumber }), queryFn: () => TaskInstanceService.getHitlDetailTryDetail({ dagId, dagRunId, mapIndex, taskId, tryNumber }) });
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
export const prefetchUseTaskInstanceServiceGetHitlDetails = (queryClient: QueryClient, { bodySearch, createdAtGt, createdAtGte, createdAtLt, createdAtLte, dagId, dagIdPattern, dagIdPrefixPattern, dagRunId, limit, mapIndex, offset, orderBy, respondedByUserId, respondedByUserName, responseReceived, state, subjectSearch, taskId, taskIdPattern, taskIdPrefixPattern }: {
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
}) => queryClient.prefetchQuery({ queryKey: Common.UseTaskInstanceServiceGetHitlDetailsKeyFn({ bodySearch, createdAtGt, createdAtGte, createdAtLt, createdAtLte, dagId, dagIdPattern, dagIdPrefixPattern, dagRunId, limit, mapIndex, offset, orderBy, respondedByUserId, respondedByUserName, responseReceived, state, subjectSearch, taskId, taskIdPattern, taskIdPrefixPattern }), queryFn: () => TaskInstanceService.getHitlDetails({ bodySearch, createdAtGt, createdAtGte, createdAtLt, createdAtLte, dagId, dagIdPattern, dagIdPrefixPattern, dagRunId, limit, mapIndex, offset, orderBy, respondedByUserId, respondedByUserName, responseReceived, state, subjectSearch, taskId, taskIdPattern, taskIdPrefixPattern }) });
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
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `id, timestamp, filename, bundle_name, stacktrace, import_error_id`
* @param data.filenamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``filename_prefix_pattern`` parameter when possible.
* @param data.filenamePrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @returns ImportErrorCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseImportErrorServiceGetImportErrors = (queryClient: QueryClient, { filenamePattern, filenamePrefixPattern, limit, offset, orderBy }: {
  filenamePattern?: string;
  filenamePrefixPattern?: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseImportErrorServiceGetImportErrorsKeyFn({ filenamePattern, filenamePrefixPattern, limit, offset, orderBy }), queryFn: () => ImportErrorService.getImportErrors({ filenamePattern, filenamePrefixPattern, limit, offset, orderBy }) });
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
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `id, pool, name`
* @param data.poolNamePattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``pool_name_prefix_pattern`` parameter when possible.
* @param data.poolNamePrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @returns PoolCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUsePoolServiceGetPools = (queryClient: QueryClient, { limit, offset, orderBy, poolNamePattern, poolNamePrefixPattern }: {
  limit?: number;
  offset?: number;
  orderBy?: string[];
  poolNamePattern?: string;
  poolNamePrefixPattern?: string;
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UsePoolServiceGetPoolsKeyFn({ limit, offset, orderBy, poolNamePattern, poolNamePrefixPattern }), queryFn: () => PoolService.getPools({ limit, offset, orderBy, poolNamePattern, poolNamePrefixPattern }) });
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
export const prefetchUseXcomServiceGetXcomEntries = (queryClient: QueryClient, { dagDisplayNamePattern, dagDisplayNamePrefixPattern, dagId, dagRunId, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, mapIndexFilter, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, runIdPrefixPattern, taskId, taskIdPattern, taskIdPrefixPattern, xcomKey, xcomKeyPattern, xcomKeyPrefixPattern }: {
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
}) => queryClient.prefetchQuery({ queryKey: Common.UseXcomServiceGetXcomEntriesKeyFn({ dagDisplayNamePattern, dagDisplayNamePrefixPattern, dagId, dagRunId, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, mapIndexFilter, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, runIdPrefixPattern, taskId, taskIdPattern, taskIdPrefixPattern, xcomKey, xcomKeyPattern, xcomKeyPrefixPattern }), queryFn: () => XcomService.getXcomEntries({ dagDisplayNamePattern, dagDisplayNamePrefixPattern, dagId, dagRunId, limit, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, mapIndex, mapIndexFilter, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runIdPattern, runIdPrefixPattern, taskId, taskIdPattern, taskIdPrefixPattern, xcomKey, xcomKeyPattern, xcomKeyPrefixPattern }) });
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
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `key, id, _val, description, is_encrypted, team_name`
* @param data.variableKeyPattern SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). or the pipe `|` operator for OR logic (e.g. `dag1 | dag2`). Regular expressions are **not** supported.
*
* **Performance note:** this full-match pattern is evaluated as ``ILIKE '%term%'`` and most of the time prevents the database from using B-tree indexes, which can be very slow on large tables. Prefer the equivalent ``variable_key_prefix_pattern`` parameter when possible.
* @param data.variableKeyPrefixPattern Prefix match — returns items whose value starts with the given string (case-sensitive, index-friendly). Use the pipe `|` operator for OR logic (e.g. `dag1|dag2`). Use `~` to match all. Wildcard characters (`%`, `_`) are treated as literal characters. Trailing non-alphanumeric characters in the prefix are stripped before matching so the range scan stays index-compatible under locale-aware collations — e.g. `test_` effectively matches items starting with `test`, and `s3://` matches items starting with `s3`.
* @returns VariableCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseVariableServiceGetVariables = (queryClient: QueryClient, { limit, offset, orderBy, variableKeyPattern, variableKeyPrefixPattern }: {
  limit?: number;
  offset?: number;
  orderBy?: string[];
  variableKeyPattern?: string;
  variableKeyPrefixPattern?: string;
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseVariableServiceGetVariablesKeyFn({ limit, offset, orderBy, variableKeyPattern, variableKeyPrefixPattern }), queryFn: () => VariableService.getVariables({ limit, offset, orderBy, variableKeyPattern, variableKeyPrefixPattern }) });
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
* @returns unknown Successful Response
* @throws ApiError
*/
export const prefetchUseLoginServiceLogout = (queryClient: QueryClient) => queryClient.prefetchQuery({ queryKey: Common.UseLoginServiceLogoutKeyFn(), queryFn: () => LoginService.logout() });
/**
* Get Auth Menus
* @returns MenuItemCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseAuthLinksServiceGetAuthMenus = (queryClient: QueryClient) => queryClient.prefetchQuery({ queryKey: Common.UseAuthLinksServiceGetAuthMenusKeyFn(), queryFn: () => AuthLinksService.getAuthMenus() });
/**
* Get Current User Info
* Convienently get the current authenticated user information.
* @returns AuthenticatedMeResponse Successful Response
* @throws ApiError
*/
export const prefetchUseAuthLinksServiceGetCurrentUserInfo = (queryClient: QueryClient) => queryClient.prefetchQuery({ queryKey: Common.UseAuthLinksServiceGetCurrentUserInfoKeyFn(), queryFn: () => AuthLinksService.getCurrentUserInfo() });
/**
* Get Partitioned Dag Runs
* Return PartitionedDagRuns. Filter by dag_id and/or has_created_dag_run_id.
* @param data The data for the request.
* @param data.dagId
* @param data.hasCreatedDagRunId
* @returns PartitionedDagRunCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUsePartitionedDagRunServiceGetPartitionedDagRuns = (queryClient: QueryClient, { dagId, hasCreatedDagRunId }: {
  dagId?: string;
  hasCreatedDagRunId?: boolean;
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UsePartitionedDagRunServiceGetPartitionedDagRunsKeyFn({ dagId, hasCreatedDagRunId }), queryFn: () => PartitionedDagRunService.getPartitionedDagRuns({ dagId, hasCreatedDagRunId }) });
/**
* Get Pending Partitioned Dag Run
* Return full details for pending PartitionedDagRun.
* @param data The data for the request.
* @param data.dagId
* @param data.partitionKey
* @returns PartitionedDagRunDetailResponse Successful Response
* @throws ApiError
*/
export const prefetchUsePartitionedDagRunServiceGetPendingPartitionedDagRun = (queryClient: QueryClient, { dagId, partitionKey }: {
  dagId: string;
  partitionKey: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UsePartitionedDagRunServiceGetPendingPartitionedDagRunKeyFn({ dagId, partitionKey }), queryFn: () => PartitionedDagRunService.getPendingPartitionedDagRun({ dagId, partitionKey }) });
/**
* Get Dependencies
* Dependencies graph.
* @param data The data for the request.
* @param data.nodeId
* @param data.dependencyType
* @returns BaseGraphResponse Successful Response
* @throws ApiError
*/
export const prefetchUseDependenciesServiceGetDependencies = (queryClient: QueryClient, { dependencyType, nodeId }: {
  dependencyType?: "scheduling" | "data";
  nodeId?: string;
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseDependenciesServiceGetDependenciesKeyFn({ dependencyType, nodeId }), queryFn: () => DependenciesService.getDependencies({ dependencyType, nodeId }) });
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
* Return basic Dag stats with counts of Dags in various states.
* @returns DashboardDagStatsResponse Successful Response
* @throws ApiError
*/
export const prefetchUseDashboardServiceDagStats = (queryClient: QueryClient) => queryClient.prefetchQuery({ queryKey: Common.UseDashboardServiceDagStatsKeyFn(), queryFn: () => DashboardService.dagStats() });
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
export const prefetchUseDeadlinesServiceGetDeadlines = (queryClient: QueryClient, { dagId, dagRunId, deadlineTimeGt, deadlineTimeGte, deadlineTimeLt, deadlineTimeLte, lastUpdatedAtGt, lastUpdatedAtGte, lastUpdatedAtLt, lastUpdatedAtLte, limit, missed, offset, orderBy }: {
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
}) => queryClient.prefetchQuery({ queryKey: Common.UseDeadlinesServiceGetDeadlinesKeyFn({ dagId, dagRunId, deadlineTimeGt, deadlineTimeGte, deadlineTimeLt, deadlineTimeLte, lastUpdatedAtGt, lastUpdatedAtGte, lastUpdatedAtLt, lastUpdatedAtLte, limit, missed, offset, orderBy }), queryFn: () => DeadlinesService.getDeadlines({ dagId, dagRunId, deadlineTimeGt, deadlineTimeGte, deadlineTimeLt, deadlineTimeLte, lastUpdatedAtGt, lastUpdatedAtGte, lastUpdatedAtLt, lastUpdatedAtLte, limit, missed, offset, orderBy }) });
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
export const prefetchUseDeadlinesServiceGetDagDeadlineAlerts = (queryClient: QueryClient, { dagId, limit, offset, orderBy }: {
  dagId: string;
  limit?: number;
  offset?: number;
  orderBy?: string[];
}) => queryClient.prefetchQuery({ queryKey: Common.UseDeadlinesServiceGetDagDeadlineAlertsKeyFn({ dagId, limit, offset, orderBy }), queryFn: () => DeadlinesService.getDagDeadlineAlerts({ dagId, limit, offset, orderBy }) });
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
export const prefetchUseStructureServiceStructureData = (queryClient: QueryClient, { dagId, depth, externalDependencies, includeDownstream, includeUpstream, root, versionNumber }: {
  dagId: string;
  depth?: number;
  externalDependencies?: boolean;
  includeDownstream?: boolean;
  includeUpstream?: boolean;
  root?: string;
  versionNumber?: number;
}) => queryClient.prefetchQuery({ queryKey: Common.UseStructureServiceStructureDataKeyFn({ dagId, depth, externalDependencies, includeDownstream, includeUpstream, root, versionNumber }), queryFn: () => StructureService.structureData({ dagId, depth, externalDependencies, includeDownstream, includeUpstream, root, versionNumber }) });
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
export const prefetchUseGridServiceGetDagStructure = (queryClient: QueryClient, { dagId, depth, includeDownstream, includeUpstream, limit, offset, orderBy, root, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runType, state, triggeringUser, triggeringUserPrefix }: {
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
}) => queryClient.prefetchQuery({ queryKey: Common.UseGridServiceGetDagStructureKeyFn({ dagId, depth, includeDownstream, includeUpstream, limit, offset, orderBy, root, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runType, state, triggeringUser, triggeringUserPrefix }), queryFn: () => GridService.getDagStructure({ dagId, depth, includeDownstream, includeUpstream, limit, offset, orderBy, root, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runType, state, triggeringUser, triggeringUserPrefix }) });
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
export const prefetchUseGridServiceGetGridRuns = (queryClient: QueryClient, { dagId, limit, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runType, state, triggeringUser, triggeringUserPrefix }: {
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
}) => queryClient.prefetchQuery({ queryKey: Common.UseGridServiceGetGridRunsKeyFn({ dagId, limit, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runType, state, triggeringUser, triggeringUserPrefix }), queryFn: () => GridService.getGridRuns({ dagId, limit, offset, orderBy, runAfterGt, runAfterGte, runAfterLt, runAfterLte, runType, state, triggeringUser, triggeringUserPrefix }) });
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
export const prefetchUseGridServiceGetGridTiSummariesStream = (queryClient: QueryClient, { dagId, runIds }: {
  dagId: string;
  runIds?: string[];
}) => queryClient.prefetchQuery({ queryKey: Common.UseGridServiceGetGridTiSummariesStreamKeyFn({ dagId, runIds }), queryFn: () => GridService.getGridTiSummariesStream({ dagId, runIds }) });
/**
* Get Gantt Data
* Get all task instance tries for Gantt chart.
* @param data The data for the request.
* @param data.dagId
* @param data.runId
* @returns GanttResponse Successful Response
* @throws ApiError
*/
export const prefetchUseGanttServiceGetGanttData = (queryClient: QueryClient, { dagId, runId }: {
  dagId: string;
  runId: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseGanttServiceGetGanttDataKeyFn({ dagId, runId }), queryFn: () => GanttService.getGanttData({ dagId, runId }) });
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
export const prefetchUseCalendarServiceGetCalendar = (queryClient: QueryClient, { dagId, granularity, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, partitionDateGt, partitionDateGte, partitionDateLt, partitionDateLte }: {
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
}) => queryClient.prefetchQuery({ queryKey: Common.UseCalendarServiceGetCalendarKeyFn({ dagId, granularity, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, partitionDateGt, partitionDateGte, partitionDateLt, partitionDateLte }), queryFn: () => CalendarService.getCalendar({ dagId, granularity, logicalDateGt, logicalDateGte, logicalDateLt, logicalDateLte, partitionDateGt, partitionDateGte, partitionDateLt, partitionDateLte }) });
/**
* List Teams
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @param data.orderBy Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. Supported attributes: `name`
* @returns TeamCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseTeamsServiceListTeams = (queryClient: QueryClient, { limit, offset, orderBy }: {
  limit?: number;
  offset?: number;
  orderBy?: string[];
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseTeamsServiceListTeamsKeyFn({ limit, offset, orderBy }), queryFn: () => TeamsService.listTeams({ limit, offset, orderBy }) });
