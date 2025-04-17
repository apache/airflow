// generated with @7nohe/openapi-react-query-codegen@1.6.2
import { UseQueryResult } from "@tanstack/react-query";

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
  DagsService,
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

export type AssetServiceGetAssetsDefaultResponse = Awaited<ReturnType<typeof AssetService.getAssets>>;
export type AssetServiceGetAssetsQueryResult<
  TData = AssetServiceGetAssetsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useAssetServiceGetAssetsKey = "AssetServiceGetAssets";
export const UseAssetServiceGetAssetsKeyFn = (
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
  queryKey?: Array<unknown>,
) => [
  useAssetServiceGetAssetsKey,
  ...(queryKey ?? [{ dagIds, limit, namePattern, offset, onlyActive, orderBy, uriPattern }]),
];
export type AssetServiceGetAssetAliasesDefaultResponse = Awaited<
  ReturnType<typeof AssetService.getAssetAliases>
>;
export type AssetServiceGetAssetAliasesQueryResult<
  TData = AssetServiceGetAssetAliasesDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useAssetServiceGetAssetAliasesKey = "AssetServiceGetAssetAliases";
export const UseAssetServiceGetAssetAliasesKeyFn = (
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
  queryKey?: Array<unknown>,
) => [useAssetServiceGetAssetAliasesKey, ...(queryKey ?? [{ limit, namePattern, offset, orderBy }])];
export type AssetServiceGetAssetAliasDefaultResponse = Awaited<ReturnType<typeof AssetService.getAssetAlias>>;
export type AssetServiceGetAssetAliasQueryResult<
  TData = AssetServiceGetAssetAliasDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useAssetServiceGetAssetAliasKey = "AssetServiceGetAssetAlias";
export const UseAssetServiceGetAssetAliasKeyFn = (
  {
    assetAliasId,
  }: {
    assetAliasId: number;
  },
  queryKey?: Array<unknown>,
) => [useAssetServiceGetAssetAliasKey, ...(queryKey ?? [{ assetAliasId }])];
export type AssetServiceGetAssetEventsDefaultResponse = Awaited<
  ReturnType<typeof AssetService.getAssetEvents>
>;
export type AssetServiceGetAssetEventsQueryResult<
  TData = AssetServiceGetAssetEventsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useAssetServiceGetAssetEventsKey = "AssetServiceGetAssetEvents";
export const UseAssetServiceGetAssetEventsKeyFn = (
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
  queryKey?: Array<unknown>,
) => [
  useAssetServiceGetAssetEventsKey,
  ...(queryKey ?? [
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
  ]),
];
export type AssetServiceGetAssetQueuedEventsDefaultResponse = Awaited<
  ReturnType<typeof AssetService.getAssetQueuedEvents>
>;
export type AssetServiceGetAssetQueuedEventsQueryResult<
  TData = AssetServiceGetAssetQueuedEventsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useAssetServiceGetAssetQueuedEventsKey = "AssetServiceGetAssetQueuedEvents";
export const UseAssetServiceGetAssetQueuedEventsKeyFn = (
  {
    assetId,
    before,
  }: {
    assetId: number;
    before?: string;
  },
  queryKey?: Array<unknown>,
) => [useAssetServiceGetAssetQueuedEventsKey, ...(queryKey ?? [{ assetId, before }])];
export type AssetServiceGetAssetDefaultResponse = Awaited<ReturnType<typeof AssetService.getAsset>>;
export type AssetServiceGetAssetQueryResult<
  TData = AssetServiceGetAssetDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useAssetServiceGetAssetKey = "AssetServiceGetAsset";
export const UseAssetServiceGetAssetKeyFn = (
  {
    assetId,
  }: {
    assetId: number;
  },
  queryKey?: Array<unknown>,
) => [useAssetServiceGetAssetKey, ...(queryKey ?? [{ assetId }])];
export type AssetServiceGetDagAssetQueuedEventsDefaultResponse = Awaited<
  ReturnType<typeof AssetService.getDagAssetQueuedEvents>
>;
export type AssetServiceGetDagAssetQueuedEventsQueryResult<
  TData = AssetServiceGetDagAssetQueuedEventsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useAssetServiceGetDagAssetQueuedEventsKey = "AssetServiceGetDagAssetQueuedEvents";
export const UseAssetServiceGetDagAssetQueuedEventsKeyFn = (
  {
    before,
    dagId,
  }: {
    before?: string;
    dagId: string;
  },
  queryKey?: Array<unknown>,
) => [useAssetServiceGetDagAssetQueuedEventsKey, ...(queryKey ?? [{ before, dagId }])];
export type AssetServiceGetDagAssetQueuedEventDefaultResponse = Awaited<
  ReturnType<typeof AssetService.getDagAssetQueuedEvent>
>;
export type AssetServiceGetDagAssetQueuedEventQueryResult<
  TData = AssetServiceGetDagAssetQueuedEventDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useAssetServiceGetDagAssetQueuedEventKey = "AssetServiceGetDagAssetQueuedEvent";
export const UseAssetServiceGetDagAssetQueuedEventKeyFn = (
  {
    assetId,
    before,
    dagId,
  }: {
    assetId: number;
    before?: string;
    dagId: string;
  },
  queryKey?: Array<unknown>,
) => [useAssetServiceGetDagAssetQueuedEventKey, ...(queryKey ?? [{ assetId, before, dagId }])];
export type AssetServiceNextRunAssetsDefaultResponse = Awaited<ReturnType<typeof AssetService.nextRunAssets>>;
export type AssetServiceNextRunAssetsQueryResult<
  TData = AssetServiceNextRunAssetsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useAssetServiceNextRunAssetsKey = "AssetServiceNextRunAssets";
export const UseAssetServiceNextRunAssetsKeyFn = (
  {
    dagId,
  }: {
    dagId: string;
  },
  queryKey?: Array<unknown>,
) => [useAssetServiceNextRunAssetsKey, ...(queryKey ?? [{ dagId }])];
export type BackfillServiceListBackfillsDefaultResponse = Awaited<
  ReturnType<typeof BackfillService.listBackfills>
>;
export type BackfillServiceListBackfillsQueryResult<
  TData = BackfillServiceListBackfillsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useBackfillServiceListBackfillsKey = "BackfillServiceListBackfills";
export const UseBackfillServiceListBackfillsKeyFn = (
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
  queryKey?: Array<unknown>,
) => [useBackfillServiceListBackfillsKey, ...(queryKey ?? [{ dagId, limit, offset, orderBy }])];
export type BackfillServiceGetBackfillDefaultResponse = Awaited<
  ReturnType<typeof BackfillService.getBackfill>
>;
export type BackfillServiceGetBackfillQueryResult<
  TData = BackfillServiceGetBackfillDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useBackfillServiceGetBackfillKey = "BackfillServiceGetBackfill";
export const UseBackfillServiceGetBackfillKeyFn = (
  {
    backfillId,
  }: {
    backfillId: string;
  },
  queryKey?: Array<unknown>,
) => [useBackfillServiceGetBackfillKey, ...(queryKey ?? [{ backfillId }])];
export type BackfillServiceListBackfills1DefaultResponse = Awaited<
  ReturnType<typeof BackfillService.listBackfills1>
>;
export type BackfillServiceListBackfills1QueryResult<
  TData = BackfillServiceListBackfills1DefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useBackfillServiceListBackfills1Key = "BackfillServiceListBackfills1";
export const UseBackfillServiceListBackfills1KeyFn = (
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
  queryKey?: Array<unknown>,
) => [useBackfillServiceListBackfills1Key, ...(queryKey ?? [{ active, dagId, limit, offset, orderBy }])];
export type ConnectionServiceGetConnectionDefaultResponse = Awaited<
  ReturnType<typeof ConnectionService.getConnection>
>;
export type ConnectionServiceGetConnectionQueryResult<
  TData = ConnectionServiceGetConnectionDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useConnectionServiceGetConnectionKey = "ConnectionServiceGetConnection";
export const UseConnectionServiceGetConnectionKeyFn = (
  {
    connectionId,
  }: {
    connectionId: string;
  },
  queryKey?: Array<unknown>,
) => [useConnectionServiceGetConnectionKey, ...(queryKey ?? [{ connectionId }])];
export type ConnectionServiceGetConnectionsDefaultResponse = Awaited<
  ReturnType<typeof ConnectionService.getConnections>
>;
export type ConnectionServiceGetConnectionsQueryResult<
  TData = ConnectionServiceGetConnectionsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useConnectionServiceGetConnectionsKey = "ConnectionServiceGetConnections";
export const UseConnectionServiceGetConnectionsKeyFn = (
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
  queryKey?: Array<unknown>,
) => [
  useConnectionServiceGetConnectionsKey,
  ...(queryKey ?? [{ connectionIdPattern, limit, offset, orderBy }]),
];
export type ConnectionServiceHookMetaDataDefaultResponse = Awaited<
  ReturnType<typeof ConnectionService.hookMetaData>
>;
export type ConnectionServiceHookMetaDataQueryResult<
  TData = ConnectionServiceHookMetaDataDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useConnectionServiceHookMetaDataKey = "ConnectionServiceHookMetaData";
export const UseConnectionServiceHookMetaDataKeyFn = (queryKey?: Array<unknown>) => [
  useConnectionServiceHookMetaDataKey,
  ...(queryKey ?? []),
];
export type DagRunServiceGetDagRunDefaultResponse = Awaited<ReturnType<typeof DagRunService.getDagRun>>;
export type DagRunServiceGetDagRunQueryResult<
  TData = DagRunServiceGetDagRunDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDagRunServiceGetDagRunKey = "DagRunServiceGetDagRun";
export const UseDagRunServiceGetDagRunKeyFn = (
  {
    dagId,
    dagRunId,
  }: {
    dagId: string;
    dagRunId: string;
  },
  queryKey?: Array<unknown>,
) => [useDagRunServiceGetDagRunKey, ...(queryKey ?? [{ dagId, dagRunId }])];
export type DagRunServiceGetUpstreamAssetEventsDefaultResponse = Awaited<
  ReturnType<typeof DagRunService.getUpstreamAssetEvents>
>;
export type DagRunServiceGetUpstreamAssetEventsQueryResult<
  TData = DagRunServiceGetUpstreamAssetEventsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDagRunServiceGetUpstreamAssetEventsKey = "DagRunServiceGetUpstreamAssetEvents";
export const UseDagRunServiceGetUpstreamAssetEventsKeyFn = (
  {
    dagId,
    dagRunId,
  }: {
    dagId: string;
    dagRunId: string;
  },
  queryKey?: Array<unknown>,
) => [useDagRunServiceGetUpstreamAssetEventsKey, ...(queryKey ?? [{ dagId, dagRunId }])];
export type DagRunServiceGetDagRunsDefaultResponse = Awaited<ReturnType<typeof DagRunService.getDagRuns>>;
export type DagRunServiceGetDagRunsQueryResult<
  TData = DagRunServiceGetDagRunsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDagRunServiceGetDagRunsKey = "DagRunServiceGetDagRuns";
export const UseDagRunServiceGetDagRunsKeyFn = (
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
  queryKey?: Array<unknown>,
) => [
  useDagRunServiceGetDagRunsKey,
  ...(queryKey ?? [
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
  ]),
];
export type DagSourceServiceGetDagSourceDefaultResponse = Awaited<
  ReturnType<typeof DagSourceService.getDagSource>
>;
export type DagSourceServiceGetDagSourceQueryResult<
  TData = DagSourceServiceGetDagSourceDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDagSourceServiceGetDagSourceKey = "DagSourceServiceGetDagSource";
export const UseDagSourceServiceGetDagSourceKeyFn = (
  {
    accept,
    dagId,
    versionNumber,
  }: {
    accept?: "application/json" | "text/plain" | "*/*";
    dagId: string;
    versionNumber?: number;
  },
  queryKey?: Array<unknown>,
) => [useDagSourceServiceGetDagSourceKey, ...(queryKey ?? [{ accept, dagId, versionNumber }])];
export type DagStatsServiceGetDagStatsDefaultResponse = Awaited<
  ReturnType<typeof DagStatsService.getDagStats>
>;
export type DagStatsServiceGetDagStatsQueryResult<
  TData = DagStatsServiceGetDagStatsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDagStatsServiceGetDagStatsKey = "DagStatsServiceGetDagStats";
export const UseDagStatsServiceGetDagStatsKeyFn = (
  {
    dagIds,
  }: {
    dagIds?: string[];
  } = {},
  queryKey?: Array<unknown>,
) => [useDagStatsServiceGetDagStatsKey, ...(queryKey ?? [{ dagIds }])];
export type DagReportServiceGetDagReportsDefaultResponse = Awaited<
  ReturnType<typeof DagReportService.getDagReports>
>;
export type DagReportServiceGetDagReportsQueryResult<
  TData = DagReportServiceGetDagReportsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDagReportServiceGetDagReportsKey = "DagReportServiceGetDagReports";
export const UseDagReportServiceGetDagReportsKeyFn = (
  {
    subdir,
  }: {
    subdir: string;
  },
  queryKey?: Array<unknown>,
) => [useDagReportServiceGetDagReportsKey, ...(queryKey ?? [{ subdir }])];
export type ConfigServiceGetConfigDefaultResponse = Awaited<ReturnType<typeof ConfigService.getConfig>>;
export type ConfigServiceGetConfigQueryResult<
  TData = ConfigServiceGetConfigDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useConfigServiceGetConfigKey = "ConfigServiceGetConfig";
export const UseConfigServiceGetConfigKeyFn = (
  {
    accept,
    section,
  }: {
    accept?: "application/json" | "text/plain" | "*/*";
    section?: string;
  } = {},
  queryKey?: Array<unknown>,
) => [useConfigServiceGetConfigKey, ...(queryKey ?? [{ accept, section }])];
export type ConfigServiceGetConfigValueDefaultResponse = Awaited<
  ReturnType<typeof ConfigService.getConfigValue>
>;
export type ConfigServiceGetConfigValueQueryResult<
  TData = ConfigServiceGetConfigValueDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useConfigServiceGetConfigValueKey = "ConfigServiceGetConfigValue";
export const UseConfigServiceGetConfigValueKeyFn = (
  {
    accept,
    option,
    section,
  }: {
    accept?: "application/json" | "text/plain" | "*/*";
    option: string;
    section: string;
  },
  queryKey?: Array<unknown>,
) => [useConfigServiceGetConfigValueKey, ...(queryKey ?? [{ accept, option, section }])];
export type ConfigServiceGetConfigsDefaultResponse = Awaited<ReturnType<typeof ConfigService.getConfigs>>;
export type ConfigServiceGetConfigsQueryResult<
  TData = ConfigServiceGetConfigsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useConfigServiceGetConfigsKey = "ConfigServiceGetConfigs";
export const UseConfigServiceGetConfigsKeyFn = (queryKey?: Array<unknown>) => [
  useConfigServiceGetConfigsKey,
  ...(queryKey ?? []),
];
export type DagWarningServiceListDagWarningsDefaultResponse = Awaited<
  ReturnType<typeof DagWarningService.listDagWarnings>
>;
export type DagWarningServiceListDagWarningsQueryResult<
  TData = DagWarningServiceListDagWarningsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDagWarningServiceListDagWarningsKey = "DagWarningServiceListDagWarnings";
export const UseDagWarningServiceListDagWarningsKeyFn = (
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
  queryKey?: Array<unknown>,
) => [
  useDagWarningServiceListDagWarningsKey,
  ...(queryKey ?? [{ dagId, limit, offset, orderBy, warningType }]),
];
export type DagServiceGetDagsDefaultResponse = Awaited<ReturnType<typeof DagService.getDags>>;
export type DagServiceGetDagsQueryResult<
  TData = DagServiceGetDagsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDagServiceGetDagsKey = "DagServiceGetDags";
export const UseDagServiceGetDagsKeyFn = (
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
  queryKey?: Array<unknown>,
) => [
  useDagServiceGetDagsKey,
  ...(queryKey ?? [
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
  ]),
];
export type DagServiceGetDagDefaultResponse = Awaited<ReturnType<typeof DagService.getDag>>;
export type DagServiceGetDagQueryResult<
  TData = DagServiceGetDagDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDagServiceGetDagKey = "DagServiceGetDag";
export const UseDagServiceGetDagKeyFn = (
  {
    dagId,
  }: {
    dagId: string;
  },
  queryKey?: Array<unknown>,
) => [useDagServiceGetDagKey, ...(queryKey ?? [{ dagId }])];
export type DagServiceGetDagDetailsDefaultResponse = Awaited<ReturnType<typeof DagService.getDagDetails>>;
export type DagServiceGetDagDetailsQueryResult<
  TData = DagServiceGetDagDetailsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDagServiceGetDagDetailsKey = "DagServiceGetDagDetails";
export const UseDagServiceGetDagDetailsKeyFn = (
  {
    dagId,
  }: {
    dagId: string;
  },
  queryKey?: Array<unknown>,
) => [useDagServiceGetDagDetailsKey, ...(queryKey ?? [{ dagId }])];
export type DagServiceGetDagTagsDefaultResponse = Awaited<ReturnType<typeof DagService.getDagTags>>;
export type DagServiceGetDagTagsQueryResult<
  TData = DagServiceGetDagTagsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDagServiceGetDagTagsKey = "DagServiceGetDagTags";
export const UseDagServiceGetDagTagsKeyFn = (
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
  queryKey?: Array<unknown>,
) => [useDagServiceGetDagTagsKey, ...(queryKey ?? [{ limit, offset, orderBy, tagNamePattern }])];
export type EventLogServiceGetEventLogDefaultResponse = Awaited<
  ReturnType<typeof EventLogService.getEventLog>
>;
export type EventLogServiceGetEventLogQueryResult<
  TData = EventLogServiceGetEventLogDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useEventLogServiceGetEventLogKey = "EventLogServiceGetEventLog";
export const UseEventLogServiceGetEventLogKeyFn = (
  {
    eventLogId,
  }: {
    eventLogId: number;
  },
  queryKey?: Array<unknown>,
) => [useEventLogServiceGetEventLogKey, ...(queryKey ?? [{ eventLogId }])];
export type EventLogServiceGetEventLogsDefaultResponse = Awaited<
  ReturnType<typeof EventLogService.getEventLogs>
>;
export type EventLogServiceGetEventLogsQueryResult<
  TData = EventLogServiceGetEventLogsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useEventLogServiceGetEventLogsKey = "EventLogServiceGetEventLogs";
export const UseEventLogServiceGetEventLogsKeyFn = (
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
  queryKey?: Array<unknown>,
) => [
  useEventLogServiceGetEventLogsKey,
  ...(queryKey ?? [
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
  ]),
];
export type ExtraLinksServiceGetExtraLinksDefaultResponse = Awaited<
  ReturnType<typeof ExtraLinksService.getExtraLinks>
>;
export type ExtraLinksServiceGetExtraLinksQueryResult<
  TData = ExtraLinksServiceGetExtraLinksDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useExtraLinksServiceGetExtraLinksKey = "ExtraLinksServiceGetExtraLinks";
export const UseExtraLinksServiceGetExtraLinksKeyFn = (
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
  queryKey?: Array<unknown>,
) => [useExtraLinksServiceGetExtraLinksKey, ...(queryKey ?? [{ dagId, dagRunId, mapIndex, taskId }])];
export type TaskInstanceServiceGetExtraLinksDefaultResponse = Awaited<
  ReturnType<typeof TaskInstanceService.getExtraLinks>
>;
export type TaskInstanceServiceGetExtraLinksQueryResult<
  TData = TaskInstanceServiceGetExtraLinksDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useTaskInstanceServiceGetExtraLinksKey = "TaskInstanceServiceGetExtraLinks";
export const UseTaskInstanceServiceGetExtraLinksKeyFn = (
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
  queryKey?: Array<unknown>,
) => [useTaskInstanceServiceGetExtraLinksKey, ...(queryKey ?? [{ dagId, dagRunId, mapIndex, taskId }])];
export type TaskInstanceServiceGetTaskInstanceDefaultResponse = Awaited<
  ReturnType<typeof TaskInstanceService.getTaskInstance>
>;
export type TaskInstanceServiceGetTaskInstanceQueryResult<
  TData = TaskInstanceServiceGetTaskInstanceDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useTaskInstanceServiceGetTaskInstanceKey = "TaskInstanceServiceGetTaskInstance";
export const UseTaskInstanceServiceGetTaskInstanceKeyFn = (
  {
    dagId,
    dagRunId,
    taskId,
  }: {
    dagId: string;
    dagRunId: string;
    taskId: string;
  },
  queryKey?: Array<unknown>,
) => [useTaskInstanceServiceGetTaskInstanceKey, ...(queryKey ?? [{ dagId, dagRunId, taskId }])];
export type TaskInstanceServiceGetMappedTaskInstancesDefaultResponse = Awaited<
  ReturnType<typeof TaskInstanceService.getMappedTaskInstances>
>;
export type TaskInstanceServiceGetMappedTaskInstancesQueryResult<
  TData = TaskInstanceServiceGetMappedTaskInstancesDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useTaskInstanceServiceGetMappedTaskInstancesKey = "TaskInstanceServiceGetMappedTaskInstances";
export const UseTaskInstanceServiceGetMappedTaskInstancesKeyFn = (
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
  queryKey?: Array<unknown>,
) => [
  useTaskInstanceServiceGetMappedTaskInstancesKey,
  ...(queryKey ?? [
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
  ]),
];
export type TaskInstanceServiceGetTaskInstanceDependenciesDefaultResponse = Awaited<
  ReturnType<typeof TaskInstanceService.getTaskInstanceDependencies>
>;
export type TaskInstanceServiceGetTaskInstanceDependenciesQueryResult<
  TData = TaskInstanceServiceGetTaskInstanceDependenciesDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useTaskInstanceServiceGetTaskInstanceDependenciesKey =
  "TaskInstanceServiceGetTaskInstanceDependencies";
export const UseTaskInstanceServiceGetTaskInstanceDependenciesKeyFn = (
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
  queryKey?: Array<unknown>,
) => [
  useTaskInstanceServiceGetTaskInstanceDependenciesKey,
  ...(queryKey ?? [{ dagId, dagRunId, mapIndex, taskId }]),
];
export type TaskInstanceServiceGetTaskInstanceDependencies1DefaultResponse = Awaited<
  ReturnType<typeof TaskInstanceService.getTaskInstanceDependencies1>
>;
export type TaskInstanceServiceGetTaskInstanceDependencies1QueryResult<
  TData = TaskInstanceServiceGetTaskInstanceDependencies1DefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useTaskInstanceServiceGetTaskInstanceDependencies1Key =
  "TaskInstanceServiceGetTaskInstanceDependencies1";
export const UseTaskInstanceServiceGetTaskInstanceDependencies1KeyFn = (
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
  queryKey?: Array<unknown>,
) => [
  useTaskInstanceServiceGetTaskInstanceDependencies1Key,
  ...(queryKey ?? [{ dagId, dagRunId, mapIndex, taskId }]),
];
export type TaskInstanceServiceGetTaskInstanceTriesDefaultResponse = Awaited<
  ReturnType<typeof TaskInstanceService.getTaskInstanceTries>
>;
export type TaskInstanceServiceGetTaskInstanceTriesQueryResult<
  TData = TaskInstanceServiceGetTaskInstanceTriesDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useTaskInstanceServiceGetTaskInstanceTriesKey = "TaskInstanceServiceGetTaskInstanceTries";
export const UseTaskInstanceServiceGetTaskInstanceTriesKeyFn = (
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
  queryKey?: Array<unknown>,
) => [
  useTaskInstanceServiceGetTaskInstanceTriesKey,
  ...(queryKey ?? [{ dagId, dagRunId, mapIndex, taskId }]),
];
export type TaskInstanceServiceGetMappedTaskInstanceTriesDefaultResponse = Awaited<
  ReturnType<typeof TaskInstanceService.getMappedTaskInstanceTries>
>;
export type TaskInstanceServiceGetMappedTaskInstanceTriesQueryResult<
  TData = TaskInstanceServiceGetMappedTaskInstanceTriesDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useTaskInstanceServiceGetMappedTaskInstanceTriesKey =
  "TaskInstanceServiceGetMappedTaskInstanceTries";
export const UseTaskInstanceServiceGetMappedTaskInstanceTriesKeyFn = (
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
  queryKey?: Array<unknown>,
) => [
  useTaskInstanceServiceGetMappedTaskInstanceTriesKey,
  ...(queryKey ?? [{ dagId, dagRunId, mapIndex, taskId }]),
];
export type TaskInstanceServiceGetMappedTaskInstanceDefaultResponse = Awaited<
  ReturnType<typeof TaskInstanceService.getMappedTaskInstance>
>;
export type TaskInstanceServiceGetMappedTaskInstanceQueryResult<
  TData = TaskInstanceServiceGetMappedTaskInstanceDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useTaskInstanceServiceGetMappedTaskInstanceKey = "TaskInstanceServiceGetMappedTaskInstance";
export const UseTaskInstanceServiceGetMappedTaskInstanceKeyFn = (
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
  queryKey?: Array<unknown>,
) => [
  useTaskInstanceServiceGetMappedTaskInstanceKey,
  ...(queryKey ?? [{ dagId, dagRunId, mapIndex, taskId }]),
];
export type TaskInstanceServiceGetTaskInstancesDefaultResponse = Awaited<
  ReturnType<typeof TaskInstanceService.getTaskInstances>
>;
export type TaskInstanceServiceGetTaskInstancesQueryResult<
  TData = TaskInstanceServiceGetTaskInstancesDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useTaskInstanceServiceGetTaskInstancesKey = "TaskInstanceServiceGetTaskInstances";
export const UseTaskInstanceServiceGetTaskInstancesKeyFn = (
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
  queryKey?: Array<unknown>,
) => [
  useTaskInstanceServiceGetTaskInstancesKey,
  ...(queryKey ?? [
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
  ]),
];
export type TaskInstanceServiceGetTaskInstanceTryDetailsDefaultResponse = Awaited<
  ReturnType<typeof TaskInstanceService.getTaskInstanceTryDetails>
>;
export type TaskInstanceServiceGetTaskInstanceTryDetailsQueryResult<
  TData = TaskInstanceServiceGetTaskInstanceTryDetailsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useTaskInstanceServiceGetTaskInstanceTryDetailsKey =
  "TaskInstanceServiceGetTaskInstanceTryDetails";
export const UseTaskInstanceServiceGetTaskInstanceTryDetailsKeyFn = (
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
  queryKey?: Array<unknown>,
) => [
  useTaskInstanceServiceGetTaskInstanceTryDetailsKey,
  ...(queryKey ?? [{ dagId, dagRunId, mapIndex, taskId, taskTryNumber }]),
];
export type TaskInstanceServiceGetMappedTaskInstanceTryDetailsDefaultResponse = Awaited<
  ReturnType<typeof TaskInstanceService.getMappedTaskInstanceTryDetails>
>;
export type TaskInstanceServiceGetMappedTaskInstanceTryDetailsQueryResult<
  TData = TaskInstanceServiceGetMappedTaskInstanceTryDetailsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useTaskInstanceServiceGetMappedTaskInstanceTryDetailsKey =
  "TaskInstanceServiceGetMappedTaskInstanceTryDetails";
export const UseTaskInstanceServiceGetMappedTaskInstanceTryDetailsKeyFn = (
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
  queryKey?: Array<unknown>,
) => [
  useTaskInstanceServiceGetMappedTaskInstanceTryDetailsKey,
  ...(queryKey ?? [{ dagId, dagRunId, mapIndex, taskId, taskTryNumber }]),
];
export type TaskInstanceServiceGetLogDefaultResponse = Awaited<ReturnType<typeof TaskInstanceService.getLog>>;
export type TaskInstanceServiceGetLogQueryResult<
  TData = TaskInstanceServiceGetLogDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useTaskInstanceServiceGetLogKey = "TaskInstanceServiceGetLog";
export const UseTaskInstanceServiceGetLogKeyFn = (
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
    accept?: "application/json" | "text/plain" | "*/*";
    dagId: string;
    dagRunId: string;
    fullContent?: boolean;
    mapIndex?: number;
    taskId: string;
    token?: string;
    tryNumber: number;
  },
  queryKey?: Array<unknown>,
) => [
  useTaskInstanceServiceGetLogKey,
  ...(queryKey ?? [{ accept, dagId, dagRunId, fullContent, mapIndex, taskId, token, tryNumber }]),
];
export type ImportErrorServiceGetImportErrorDefaultResponse = Awaited<
  ReturnType<typeof ImportErrorService.getImportError>
>;
export type ImportErrorServiceGetImportErrorQueryResult<
  TData = ImportErrorServiceGetImportErrorDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useImportErrorServiceGetImportErrorKey = "ImportErrorServiceGetImportError";
export const UseImportErrorServiceGetImportErrorKeyFn = (
  {
    importErrorId,
  }: {
    importErrorId: number;
  },
  queryKey?: Array<unknown>,
) => [useImportErrorServiceGetImportErrorKey, ...(queryKey ?? [{ importErrorId }])];
export type ImportErrorServiceGetImportErrorsDefaultResponse = Awaited<
  ReturnType<typeof ImportErrorService.getImportErrors>
>;
export type ImportErrorServiceGetImportErrorsQueryResult<
  TData = ImportErrorServiceGetImportErrorsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useImportErrorServiceGetImportErrorsKey = "ImportErrorServiceGetImportErrors";
export const UseImportErrorServiceGetImportErrorsKeyFn = (
  {
    limit,
    offset,
    orderBy,
  }: {
    limit?: number;
    offset?: number;
    orderBy?: string;
  } = {},
  queryKey?: Array<unknown>,
) => [useImportErrorServiceGetImportErrorsKey, ...(queryKey ?? [{ limit, offset, orderBy }])];
export type JobServiceGetJobsDefaultResponse = Awaited<ReturnType<typeof JobService.getJobs>>;
export type JobServiceGetJobsQueryResult<
  TData = JobServiceGetJobsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useJobServiceGetJobsKey = "JobServiceGetJobs";
export const UseJobServiceGetJobsKeyFn = (
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
  queryKey?: Array<unknown>,
) => [
  useJobServiceGetJobsKey,
  ...(queryKey ?? [
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
  ]),
];
export type PluginServiceGetPluginsDefaultResponse = Awaited<ReturnType<typeof PluginService.getPlugins>>;
export type PluginServiceGetPluginsQueryResult<
  TData = PluginServiceGetPluginsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const usePluginServiceGetPluginsKey = "PluginServiceGetPlugins";
export const UsePluginServiceGetPluginsKeyFn = (
  {
    limit,
    offset,
  }: {
    limit?: number;
    offset?: number;
  } = {},
  queryKey?: Array<unknown>,
) => [usePluginServiceGetPluginsKey, ...(queryKey ?? [{ limit, offset }])];
export type PoolServiceGetPoolDefaultResponse = Awaited<ReturnType<typeof PoolService.getPool>>;
export type PoolServiceGetPoolQueryResult<
  TData = PoolServiceGetPoolDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const usePoolServiceGetPoolKey = "PoolServiceGetPool";
export const UsePoolServiceGetPoolKeyFn = (
  {
    poolName,
  }: {
    poolName: string;
  },
  queryKey?: Array<unknown>,
) => [usePoolServiceGetPoolKey, ...(queryKey ?? [{ poolName }])];
export type PoolServiceGetPoolsDefaultResponse = Awaited<ReturnType<typeof PoolService.getPools>>;
export type PoolServiceGetPoolsQueryResult<
  TData = PoolServiceGetPoolsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const usePoolServiceGetPoolsKey = "PoolServiceGetPools";
export const UsePoolServiceGetPoolsKeyFn = (
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
  queryKey?: Array<unknown>,
) => [usePoolServiceGetPoolsKey, ...(queryKey ?? [{ limit, offset, orderBy, poolNamePattern }])];
export type ProviderServiceGetProvidersDefaultResponse = Awaited<
  ReturnType<typeof ProviderService.getProviders>
>;
export type ProviderServiceGetProvidersQueryResult<
  TData = ProviderServiceGetProvidersDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useProviderServiceGetProvidersKey = "ProviderServiceGetProviders";
export const UseProviderServiceGetProvidersKeyFn = (
  {
    limit,
    offset,
  }: {
    limit?: number;
    offset?: number;
  } = {},
  queryKey?: Array<unknown>,
) => [useProviderServiceGetProvidersKey, ...(queryKey ?? [{ limit, offset }])];
export type XcomServiceGetXcomEntryDefaultResponse = Awaited<ReturnType<typeof XcomService.getXcomEntry>>;
export type XcomServiceGetXcomEntryQueryResult<
  TData = XcomServiceGetXcomEntryDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useXcomServiceGetXcomEntryKey = "XcomServiceGetXcomEntry";
export const UseXcomServiceGetXcomEntryKeyFn = (
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
  queryKey?: Array<unknown>,
) => [
  useXcomServiceGetXcomEntryKey,
  ...(queryKey ?? [{ dagId, dagRunId, deserialize, mapIndex, stringify, taskId, xcomKey }]),
];
export type XcomServiceGetXcomEntriesDefaultResponse = Awaited<ReturnType<typeof XcomService.getXcomEntries>>;
export type XcomServiceGetXcomEntriesQueryResult<
  TData = XcomServiceGetXcomEntriesDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useXcomServiceGetXcomEntriesKey = "XcomServiceGetXcomEntries";
export const UseXcomServiceGetXcomEntriesKeyFn = (
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
  queryKey?: Array<unknown>,
) => [
  useXcomServiceGetXcomEntriesKey,
  ...(queryKey ?? [{ dagId, dagRunId, limit, mapIndex, offset, taskId, xcomKey }]),
];
export type TaskServiceGetTasksDefaultResponse = Awaited<ReturnType<typeof TaskService.getTasks>>;
export type TaskServiceGetTasksQueryResult<
  TData = TaskServiceGetTasksDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useTaskServiceGetTasksKey = "TaskServiceGetTasks";
export const UseTaskServiceGetTasksKeyFn = (
  {
    dagId,
    orderBy,
  }: {
    dagId: string;
    orderBy?: string;
  },
  queryKey?: Array<unknown>,
) => [useTaskServiceGetTasksKey, ...(queryKey ?? [{ dagId, orderBy }])];
export type TaskServiceGetTaskDefaultResponse = Awaited<ReturnType<typeof TaskService.getTask>>;
export type TaskServiceGetTaskQueryResult<
  TData = TaskServiceGetTaskDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useTaskServiceGetTaskKey = "TaskServiceGetTask";
export const UseTaskServiceGetTaskKeyFn = (
  {
    dagId,
    taskId,
  }: {
    dagId: string;
    taskId: unknown;
  },
  queryKey?: Array<unknown>,
) => [useTaskServiceGetTaskKey, ...(queryKey ?? [{ dagId, taskId }])];
export type VariableServiceGetVariableDefaultResponse = Awaited<
  ReturnType<typeof VariableService.getVariable>
>;
export type VariableServiceGetVariableQueryResult<
  TData = VariableServiceGetVariableDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useVariableServiceGetVariableKey = "VariableServiceGetVariable";
export const UseVariableServiceGetVariableKeyFn = (
  {
    variableKey,
  }: {
    variableKey: string;
  },
  queryKey?: Array<unknown>,
) => [useVariableServiceGetVariableKey, ...(queryKey ?? [{ variableKey }])];
export type VariableServiceGetVariablesDefaultResponse = Awaited<
  ReturnType<typeof VariableService.getVariables>
>;
export type VariableServiceGetVariablesQueryResult<
  TData = VariableServiceGetVariablesDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useVariableServiceGetVariablesKey = "VariableServiceGetVariables";
export const UseVariableServiceGetVariablesKeyFn = (
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
  queryKey?: Array<unknown>,
) => [useVariableServiceGetVariablesKey, ...(queryKey ?? [{ limit, offset, orderBy, variableKeyPattern }])];
export type DagVersionServiceGetDagVersionDefaultResponse = Awaited<
  ReturnType<typeof DagVersionService.getDagVersion>
>;
export type DagVersionServiceGetDagVersionQueryResult<
  TData = DagVersionServiceGetDagVersionDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDagVersionServiceGetDagVersionKey = "DagVersionServiceGetDagVersion";
export const UseDagVersionServiceGetDagVersionKeyFn = (
  {
    dagId,
    versionNumber,
  }: {
    dagId: string;
    versionNumber: number;
  },
  queryKey?: Array<unknown>,
) => [useDagVersionServiceGetDagVersionKey, ...(queryKey ?? [{ dagId, versionNumber }])];
export type DagVersionServiceGetDagVersionsDefaultResponse = Awaited<
  ReturnType<typeof DagVersionService.getDagVersions>
>;
export type DagVersionServiceGetDagVersionsQueryResult<
  TData = DagVersionServiceGetDagVersionsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDagVersionServiceGetDagVersionsKey = "DagVersionServiceGetDagVersions";
export const UseDagVersionServiceGetDagVersionsKeyFn = (
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
  queryKey?: Array<unknown>,
) => [
  useDagVersionServiceGetDagVersionsKey,
  ...(queryKey ?? [{ bundleName, bundleVersion, dagId, limit, offset, orderBy, versionNumber }]),
];
export type MonitorServiceGetHealthDefaultResponse = Awaited<ReturnType<typeof MonitorService.getHealth>>;
export type MonitorServiceGetHealthQueryResult<
  TData = MonitorServiceGetHealthDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useMonitorServiceGetHealthKey = "MonitorServiceGetHealth";
export const UseMonitorServiceGetHealthKeyFn = (queryKey?: Array<unknown>) => [
  useMonitorServiceGetHealthKey,
  ...(queryKey ?? []),
];
export type VersionServiceGetVersionDefaultResponse = Awaited<ReturnType<typeof VersionService.getVersion>>;
export type VersionServiceGetVersionQueryResult<
  TData = VersionServiceGetVersionDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useVersionServiceGetVersionKey = "VersionServiceGetVersion";
export const UseVersionServiceGetVersionKeyFn = (queryKey?: Array<unknown>) => [
  useVersionServiceGetVersionKey,
  ...(queryKey ?? []),
];
export type LoginServiceLoginDefaultResponse = Awaited<ReturnType<typeof LoginService.login>>;
export type LoginServiceLoginQueryResult<
  TData = LoginServiceLoginDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useLoginServiceLoginKey = "LoginServiceLogin";
export const UseLoginServiceLoginKeyFn = (
  {
    next,
  }: {
    next?: string;
  } = {},
  queryKey?: Array<unknown>,
) => [useLoginServiceLoginKey, ...(queryKey ?? [{ next }])];
export type LoginServiceLogoutDefaultResponse = Awaited<ReturnType<typeof LoginService.logout>>;
export type LoginServiceLogoutQueryResult<
  TData = LoginServiceLogoutDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useLoginServiceLogoutKey = "LoginServiceLogout";
export const UseLoginServiceLogoutKeyFn = (
  {
    next,
  }: {
    next?: string;
  } = {},
  queryKey?: Array<unknown>,
) => [useLoginServiceLogoutKey, ...(queryKey ?? [{ next }])];
export type AuthLinksServiceGetAuthMenusDefaultResponse = Awaited<
  ReturnType<typeof AuthLinksService.getAuthMenus>
>;
export type AuthLinksServiceGetAuthMenusQueryResult<
  TData = AuthLinksServiceGetAuthMenusDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useAuthLinksServiceGetAuthMenusKey = "AuthLinksServiceGetAuthMenus";
export const UseAuthLinksServiceGetAuthMenusKeyFn = (queryKey?: Array<unknown>) => [
  useAuthLinksServiceGetAuthMenusKey,
  ...(queryKey ?? []),
];
export type DagsServiceRecentDagRunsDefaultResponse = Awaited<ReturnType<typeof DagsService.recentDagRuns>>;
export type DagsServiceRecentDagRunsQueryResult<
  TData = DagsServiceRecentDagRunsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDagsServiceRecentDagRunsKey = "DagsServiceRecentDagRuns";
export const UseDagsServiceRecentDagRunsKeyFn = (
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
  queryKey?: Array<unknown>,
) => [
  useDagsServiceRecentDagRunsKey,
  ...(queryKey ?? [
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
  ]),
];
export type DependenciesServiceGetDependenciesDefaultResponse = Awaited<
  ReturnType<typeof DependenciesService.getDependencies>
>;
export type DependenciesServiceGetDependenciesQueryResult<
  TData = DependenciesServiceGetDependenciesDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDependenciesServiceGetDependenciesKey = "DependenciesServiceGetDependencies";
export const UseDependenciesServiceGetDependenciesKeyFn = (
  {
    nodeId,
  }: {
    nodeId?: string;
  } = {},
  queryKey?: Array<unknown>,
) => [useDependenciesServiceGetDependenciesKey, ...(queryKey ?? [{ nodeId }])];
export type DashboardServiceHistoricalMetricsDefaultResponse = Awaited<
  ReturnType<typeof DashboardService.historicalMetrics>
>;
export type DashboardServiceHistoricalMetricsQueryResult<
  TData = DashboardServiceHistoricalMetricsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDashboardServiceHistoricalMetricsKey = "DashboardServiceHistoricalMetrics";
export const UseDashboardServiceHistoricalMetricsKeyFn = (
  {
    endDate,
    startDate,
  }: {
    endDate?: string;
    startDate: string;
  },
  queryKey?: Array<unknown>,
) => [useDashboardServiceHistoricalMetricsKey, ...(queryKey ?? [{ endDate, startDate }])];
export type StructureServiceStructureDataDefaultResponse = Awaited<
  ReturnType<typeof StructureService.structureData>
>;
export type StructureServiceStructureDataQueryResult<
  TData = StructureServiceStructureDataDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useStructureServiceStructureDataKey = "StructureServiceStructureData";
export const UseStructureServiceStructureDataKeyFn = (
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
  queryKey?: Array<unknown>,
) => [
  useStructureServiceStructureDataKey,
  ...(queryKey ?? [{ dagId, externalDependencies, includeDownstream, includeUpstream, root, versionNumber }]),
];
export type GridServiceGridDataDefaultResponse = Awaited<ReturnType<typeof GridService.gridData>>;
export type GridServiceGridDataQueryResult<
  TData = GridServiceGridDataDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useGridServiceGridDataKey = "GridServiceGridData";
export const UseGridServiceGridDataKeyFn = (
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
  queryKey?: Array<unknown>,
) => [
  useGridServiceGridDataKey,
  ...(queryKey ?? [
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
  ]),
];
export type AssetServiceCreateAssetEventMutationResult = Awaited<
  ReturnType<typeof AssetService.createAssetEvent>
>;
export type AssetServiceMaterializeAssetMutationResult = Awaited<
  ReturnType<typeof AssetService.materializeAsset>
>;
export type BackfillServiceCreateBackfillMutationResult = Awaited<
  ReturnType<typeof BackfillService.createBackfill>
>;
export type BackfillServiceCreateBackfillDryRunMutationResult = Awaited<
  ReturnType<typeof BackfillService.createBackfillDryRun>
>;
export type ConnectionServicePostConnectionMutationResult = Awaited<
  ReturnType<typeof ConnectionService.postConnection>
>;
export type ConnectionServiceTestConnectionMutationResult = Awaited<
  ReturnType<typeof ConnectionService.testConnection>
>;
export type ConnectionServiceCreateDefaultConnectionsMutationResult = Awaited<
  ReturnType<typeof ConnectionService.createDefaultConnections>
>;
export type DagRunServiceClearDagRunMutationResult = Awaited<ReturnType<typeof DagRunService.clearDagRun>>;
export type DagRunServiceTriggerDagRunMutationResult = Awaited<
  ReturnType<typeof DagRunService.triggerDagRun>
>;
export type DagRunServiceGetListDagRunsBatchMutationResult = Awaited<
  ReturnType<typeof DagRunService.getListDagRunsBatch>
>;
export type TaskInstanceServiceGetTaskInstancesBatchMutationResult = Awaited<
  ReturnType<typeof TaskInstanceService.getTaskInstancesBatch>
>;
export type TaskInstanceServicePostClearTaskInstancesMutationResult = Awaited<
  ReturnType<typeof TaskInstanceService.postClearTaskInstances>
>;
export type PoolServicePostPoolMutationResult = Awaited<ReturnType<typeof PoolService.postPool>>;
export type XcomServiceCreateXcomEntryMutationResult = Awaited<
  ReturnType<typeof XcomService.createXcomEntry>
>;
export type VariableServicePostVariableMutationResult = Awaited<
  ReturnType<typeof VariableService.postVariable>
>;
export type BackfillServicePauseBackfillMutationResult = Awaited<
  ReturnType<typeof BackfillService.pauseBackfill>
>;
export type BackfillServiceUnpauseBackfillMutationResult = Awaited<
  ReturnType<typeof BackfillService.unpauseBackfill>
>;
export type BackfillServiceCancelBackfillMutationResult = Awaited<
  ReturnType<typeof BackfillService.cancelBackfill>
>;
export type DagParsingServiceReparseDagFileMutationResult = Awaited<
  ReturnType<typeof DagParsingService.reparseDagFile>
>;
export type ConnectionServicePatchConnectionMutationResult = Awaited<
  ReturnType<typeof ConnectionService.patchConnection>
>;
export type ConnectionServiceBulkConnectionsMutationResult = Awaited<
  ReturnType<typeof ConnectionService.bulkConnections>
>;
export type DagRunServicePatchDagRunMutationResult = Awaited<ReturnType<typeof DagRunService.patchDagRun>>;
export type DagServicePatchDagsMutationResult = Awaited<ReturnType<typeof DagService.patchDags>>;
export type DagServicePatchDagMutationResult = Awaited<ReturnType<typeof DagService.patchDag>>;
export type TaskInstanceServicePatchTaskInstanceMutationResult = Awaited<
  ReturnType<typeof TaskInstanceService.patchTaskInstance>
>;
export type TaskInstanceServicePatchTaskInstance1MutationResult = Awaited<
  ReturnType<typeof TaskInstanceService.patchTaskInstance1>
>;
export type TaskInstanceServicePatchTaskInstanceDryRunMutationResult = Awaited<
  ReturnType<typeof TaskInstanceService.patchTaskInstanceDryRun>
>;
export type TaskInstanceServicePatchTaskInstanceDryRun1MutationResult = Awaited<
  ReturnType<typeof TaskInstanceService.patchTaskInstanceDryRun1>
>;
export type PoolServicePatchPoolMutationResult = Awaited<ReturnType<typeof PoolService.patchPool>>;
export type PoolServiceBulkPoolsMutationResult = Awaited<ReturnType<typeof PoolService.bulkPools>>;
export type XcomServiceUpdateXcomEntryMutationResult = Awaited<
  ReturnType<typeof XcomService.updateXcomEntry>
>;
export type VariableServicePatchVariableMutationResult = Awaited<
  ReturnType<typeof VariableService.patchVariable>
>;
export type VariableServiceBulkVariablesMutationResult = Awaited<
  ReturnType<typeof VariableService.bulkVariables>
>;
export type AssetServiceDeleteAssetQueuedEventsMutationResult = Awaited<
  ReturnType<typeof AssetService.deleteAssetQueuedEvents>
>;
export type AssetServiceDeleteDagAssetQueuedEventsMutationResult = Awaited<
  ReturnType<typeof AssetService.deleteDagAssetQueuedEvents>
>;
export type AssetServiceDeleteDagAssetQueuedEventMutationResult = Awaited<
  ReturnType<typeof AssetService.deleteDagAssetQueuedEvent>
>;
export type ConnectionServiceDeleteConnectionMutationResult = Awaited<
  ReturnType<typeof ConnectionService.deleteConnection>
>;
export type DagRunServiceDeleteDagRunMutationResult = Awaited<ReturnType<typeof DagRunService.deleteDagRun>>;
export type DagServiceDeleteDagMutationResult = Awaited<ReturnType<typeof DagService.deleteDag>>;
export type PoolServiceDeletePoolMutationResult = Awaited<ReturnType<typeof PoolService.deletePool>>;
export type VariableServiceDeleteVariableMutationResult = Awaited<
  ReturnType<typeof VariableService.deleteVariable>
>;
