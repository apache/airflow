// generated with @7nohe/openapi-react-query-codegen@1.6.0
import { UseQueryResult } from "@tanstack/react-query";

import {
  AssetService,
  BackfillService,
  ConfigService,
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
  JobService,
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
import { DagRunState, DagWarningType } from "../requests/types.gen";

export type AssetServiceNextRunAssetsDefaultResponse = Awaited<
  ReturnType<typeof AssetService.nextRunAssets>
>;
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
export type AssetServiceGetAssetsDefaultResponse = Awaited<
  ReturnType<typeof AssetService.getAssets>
>;
export type AssetServiceGetAssetsQueryResult<
  TData = AssetServiceGetAssetsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useAssetServiceGetAssetsKey = "AssetServiceGetAssets";
export const UseAssetServiceGetAssetsKeyFn = (
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
  queryKey?: Array<unknown>,
) => [
  useAssetServiceGetAssetsKey,
  ...(queryKey ?? [{ dagIds, limit, offset, orderBy, uriPattern }]),
];
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
export const useAssetServiceGetAssetQueuedEventsKey =
  "AssetServiceGetAssetQueuedEvents";
export const UseAssetServiceGetAssetQueuedEventsKeyFn = (
  {
    before,
    uri,
  }: {
    before?: string;
    uri: string;
  },
  queryKey?: Array<unknown>,
) => [
  useAssetServiceGetAssetQueuedEventsKey,
  ...(queryKey ?? [{ before, uri }]),
];
export type AssetServiceGetAssetDefaultResponse = Awaited<
  ReturnType<typeof AssetService.getAsset>
>;
export type AssetServiceGetAssetQueryResult<
  TData = AssetServiceGetAssetDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useAssetServiceGetAssetKey = "AssetServiceGetAsset";
export const UseAssetServiceGetAssetKeyFn = (
  {
    uri,
  }: {
    uri: string;
  },
  queryKey?: Array<unknown>,
) => [useAssetServiceGetAssetKey, ...(queryKey ?? [{ uri }])];
export type AssetServiceGetDagAssetQueuedEventsDefaultResponse = Awaited<
  ReturnType<typeof AssetService.getDagAssetQueuedEvents>
>;
export type AssetServiceGetDagAssetQueuedEventsQueryResult<
  TData = AssetServiceGetDagAssetQueuedEventsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useAssetServiceGetDagAssetQueuedEventsKey =
  "AssetServiceGetDagAssetQueuedEvents";
export const UseAssetServiceGetDagAssetQueuedEventsKeyFn = (
  {
    before,
    dagId,
  }: {
    before?: string;
    dagId: string;
  },
  queryKey?: Array<unknown>,
) => [
  useAssetServiceGetDagAssetQueuedEventsKey,
  ...(queryKey ?? [{ before, dagId }]),
];
export type AssetServiceGetDagAssetQueuedEventDefaultResponse = Awaited<
  ReturnType<typeof AssetService.getDagAssetQueuedEvent>
>;
export type AssetServiceGetDagAssetQueuedEventQueryResult<
  TData = AssetServiceGetDagAssetQueuedEventDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useAssetServiceGetDagAssetQueuedEventKey =
  "AssetServiceGetDagAssetQueuedEvent";
export const UseAssetServiceGetDagAssetQueuedEventKeyFn = (
  {
    before,
    dagId,
    uri,
  }: {
    before?: string;
    dagId: string;
    uri: string;
  },
  queryKey?: Array<unknown>,
) => [
  useAssetServiceGetDagAssetQueuedEventKey,
  ...(queryKey ?? [{ before, dagId, uri }]),
];
export type DashboardServiceHistoricalMetricsDefaultResponse = Awaited<
  ReturnType<typeof DashboardService.historicalMetrics>
>;
export type DashboardServiceHistoricalMetricsQueryResult<
  TData = DashboardServiceHistoricalMetricsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDashboardServiceHistoricalMetricsKey =
  "DashboardServiceHistoricalMetrics";
export const UseDashboardServiceHistoricalMetricsKeyFn = (
  {
    endDate,
    startDate,
  }: {
    endDate?: string;
    startDate: string;
  },
  queryKey?: Array<unknown>,
) => [
  useDashboardServiceHistoricalMetricsKey,
  ...(queryKey ?? [{ endDate, startDate }]),
];
export type DagsServiceRecentDagRunsDefaultResponse = Awaited<
  ReturnType<typeof DagsService.recentDagRuns>
>;
export type DagsServiceRecentDagRunsQueryResult<
  TData = DagsServiceRecentDagRunsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDagsServiceRecentDagRunsKey = "DagsServiceRecentDagRuns";
export const UseDagsServiceRecentDagRunsKeyFn = (
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
  queryKey?: Array<unknown>,
) => [
  useDagsServiceRecentDagRunsKey,
  ...(queryKey ?? [
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
  ]),
];
export type ConfigServiceGetConfigsDefaultResponse = Awaited<
  ReturnType<typeof ConfigService.getConfigs>
>;
export type ConfigServiceGetConfigsQueryResult<
  TData = ConfigServiceGetConfigsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useConfigServiceGetConfigsKey = "ConfigServiceGetConfigs";
export const UseConfigServiceGetConfigsKeyFn = (queryKey?: Array<unknown>) => [
  useConfigServiceGetConfigsKey,
  ...(queryKey ?? []),
];
export type ConfigServiceGetConfigDefaultResponse = Awaited<
  ReturnType<typeof ConfigService.getConfig>
>;
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
) => [
  useConfigServiceGetConfigValueKey,
  ...(queryKey ?? [{ accept, option, section }]),
];
export type BackfillServiceListBackfillsDefaultResponse = Awaited<
  ReturnType<typeof BackfillService.listBackfills>
>;
export type BackfillServiceListBackfillsQueryResult<
  TData = BackfillServiceListBackfillsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useBackfillServiceListBackfillsKey =
  "BackfillServiceListBackfills";
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
) => [
  useBackfillServiceListBackfillsKey,
  ...(queryKey ?? [{ dagId, limit, offset, orderBy }]),
];
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
export type ConnectionServiceGetConnectionDefaultResponse = Awaited<
  ReturnType<typeof ConnectionService.getConnection>
>;
export type ConnectionServiceGetConnectionQueryResult<
  TData = ConnectionServiceGetConnectionDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useConnectionServiceGetConnectionKey =
  "ConnectionServiceGetConnection";
export const UseConnectionServiceGetConnectionKeyFn = (
  {
    connectionId,
  }: {
    connectionId: string;
  },
  queryKey?: Array<unknown>,
) => [
  useConnectionServiceGetConnectionKey,
  ...(queryKey ?? [{ connectionId }]),
];
export type ConnectionServiceGetConnectionsDefaultResponse = Awaited<
  ReturnType<typeof ConnectionService.getConnections>
>;
export type ConnectionServiceGetConnectionsQueryResult<
  TData = ConnectionServiceGetConnectionsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useConnectionServiceGetConnectionsKey =
  "ConnectionServiceGetConnections";
export const UseConnectionServiceGetConnectionsKeyFn = (
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
) => [
  useConnectionServiceGetConnectionsKey,
  ...(queryKey ?? [{ limit, offset, orderBy }]),
];
export type DagRunServiceGetDagRunDefaultResponse = Awaited<
  ReturnType<typeof DagRunService.getDagRun>
>;
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
export const useDagRunServiceGetUpstreamAssetEventsKey =
  "DagRunServiceGetUpstreamAssetEvents";
export const UseDagRunServiceGetUpstreamAssetEventsKeyFn = (
  {
    dagId,
    dagRunId,
  }: {
    dagId: string;
    dagRunId: string;
  },
  queryKey?: Array<unknown>,
) => [
  useDagRunServiceGetUpstreamAssetEventsKey,
  ...(queryKey ?? [{ dagId, dagRunId }]),
];
export type DagRunServiceGetDagRunsDefaultResponse = Awaited<
  ReturnType<typeof DagRunService.getDagRuns>
>;
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
export const useDagSourceServiceGetDagSourceKey =
  "DagSourceServiceGetDagSource";
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
) => [
  useDagSourceServiceGetDagSourceKey,
  ...(queryKey ?? [{ accept, dagId, versionNumber }]),
];
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
export type DagWarningServiceListDagWarningsDefaultResponse = Awaited<
  ReturnType<typeof DagWarningService.listDagWarnings>
>;
export type DagWarningServiceListDagWarningsQueryResult<
  TData = DagWarningServiceListDagWarningsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDagWarningServiceListDagWarningsKey =
  "DagWarningServiceListDagWarnings";
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
export type DagServiceGetDagsDefaultResponse = Awaited<
  ReturnType<typeof DagService.getDags>
>;
export type DagServiceGetDagsQueryResult<
  TData = DagServiceGetDagsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDagServiceGetDagsKey = "DagServiceGetDags";
export const UseDagServiceGetDagsKeyFn = (
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
  queryKey?: Array<unknown>,
) => [
  useDagServiceGetDagsKey,
  ...(queryKey ?? [
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
  ]),
];
export type DagServiceGetDagTagsDefaultResponse = Awaited<
  ReturnType<typeof DagService.getDagTags>
>;
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
) => [
  useDagServiceGetDagTagsKey,
  ...(queryKey ?? [{ limit, offset, orderBy, tagNamePattern }]),
];
export type DagServiceGetDagDefaultResponse = Awaited<
  ReturnType<typeof DagService.getDag>
>;
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
export type DagServiceGetDagDetailsDefaultResponse = Awaited<
  ReturnType<typeof DagService.getDagDetails>
>;
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
export type ImportErrorServiceGetImportErrorDefaultResponse = Awaited<
  ReturnType<typeof ImportErrorService.getImportError>
>;
export type ImportErrorServiceGetImportErrorQueryResult<
  TData = ImportErrorServiceGetImportErrorDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useImportErrorServiceGetImportErrorKey =
  "ImportErrorServiceGetImportError";
export const UseImportErrorServiceGetImportErrorKeyFn = (
  {
    importErrorId,
  }: {
    importErrorId: number;
  },
  queryKey?: Array<unknown>,
) => [
  useImportErrorServiceGetImportErrorKey,
  ...(queryKey ?? [{ importErrorId }]),
];
export type ImportErrorServiceGetImportErrorsDefaultResponse = Awaited<
  ReturnType<typeof ImportErrorService.getImportErrors>
>;
export type ImportErrorServiceGetImportErrorsQueryResult<
  TData = ImportErrorServiceGetImportErrorsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useImportErrorServiceGetImportErrorsKey =
  "ImportErrorServiceGetImportErrors";
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
) => [
  useImportErrorServiceGetImportErrorsKey,
  ...(queryKey ?? [{ limit, offset, orderBy }]),
];
export type JobServiceGetJobsDefaultResponse = Awaited<
  ReturnType<typeof JobService.getJobs>
>;
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
export type PluginServiceGetPluginsDefaultResponse = Awaited<
  ReturnType<typeof PluginService.getPlugins>
>;
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
export type PoolServiceGetPoolDefaultResponse = Awaited<
  ReturnType<typeof PoolService.getPool>
>;
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
export type PoolServiceGetPoolsDefaultResponse = Awaited<
  ReturnType<typeof PoolService.getPools>
>;
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
  }: {
    limit?: number;
    offset?: number;
    orderBy?: string;
  } = {},
  queryKey?: Array<unknown>,
) => [usePoolServiceGetPoolsKey, ...(queryKey ?? [{ limit, offset, orderBy }])];
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
export type TaskInstanceServiceGetTaskInstanceDefaultResponse = Awaited<
  ReturnType<typeof TaskInstanceService.getTaskInstance>
>;
export type TaskInstanceServiceGetTaskInstanceQueryResult<
  TData = TaskInstanceServiceGetTaskInstanceDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useTaskInstanceServiceGetTaskInstanceKey =
  "TaskInstanceServiceGetTaskInstance";
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
) => [
  useTaskInstanceServiceGetTaskInstanceKey,
  ...(queryKey ?? [{ dagId, dagRunId, taskId }]),
];
export type TaskInstanceServiceGetMappedTaskInstancesDefaultResponse = Awaited<
  ReturnType<typeof TaskInstanceService.getMappedTaskInstances>
>;
export type TaskInstanceServiceGetMappedTaskInstancesQueryResult<
  TData = TaskInstanceServiceGetMappedTaskInstancesDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useTaskInstanceServiceGetMappedTaskInstancesKey =
  "TaskInstanceServiceGetMappedTaskInstances";
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
      startDateGte,
      startDateLte,
      state,
      taskId,
      updatedAtGte,
      updatedAtLte,
    },
  ]),
];
export type TaskInstanceServiceGetTaskInstanceDependenciesDefaultResponse =
  Awaited<ReturnType<typeof TaskInstanceService.getTaskInstanceDependencies>>;
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
export type TaskInstanceServiceGetTaskInstanceDependencies1DefaultResponse =
  Awaited<ReturnType<typeof TaskInstanceService.getTaskInstanceDependencies1>>;
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
export const useTaskInstanceServiceGetTaskInstanceTriesKey =
  "TaskInstanceServiceGetTaskInstanceTries";
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
export type TaskInstanceServiceGetMappedTaskInstanceDefaultResponse = Awaited<
  ReturnType<typeof TaskInstanceService.getMappedTaskInstance>
>;
export type TaskInstanceServiceGetMappedTaskInstanceQueryResult<
  TData = TaskInstanceServiceGetMappedTaskInstanceDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useTaskInstanceServiceGetMappedTaskInstanceKey =
  "TaskInstanceServiceGetMappedTaskInstance";
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
export const useTaskInstanceServiceGetTaskInstancesKey =
  "TaskInstanceServiceGetTaskInstances";
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
      startDateGte,
      startDateLte,
      state,
      updatedAtGte,
      updatedAtLte,
    },
  ]),
];
export type TaskInstanceServiceGetTaskInstanceTryDetailsDefaultResponse =
  Awaited<ReturnType<typeof TaskInstanceService.getTaskInstanceTryDetails>>;
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
export type TaskInstanceServiceGetMappedTaskInstanceTryDetailsDefaultResponse =
  Awaited<
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
export type TaskInstanceServiceGetLogDefaultResponse = Awaited<
  ReturnType<typeof TaskInstanceService.getLog>
>;
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
  ...(queryKey ?? [
    {
      accept,
      dagId,
      dagRunId,
      fullContent,
      mapIndex,
      taskId,
      token,
      tryNumber,
    },
  ]),
];
export type TaskServiceGetTasksDefaultResponse = Awaited<
  ReturnType<typeof TaskService.getTasks>
>;
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
export type TaskServiceGetTaskDefaultResponse = Awaited<
  ReturnType<typeof TaskService.getTask>
>;
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
  }: {
    limit?: number;
    offset?: number;
    orderBy?: string;
  } = {},
  queryKey?: Array<unknown>,
) => [
  useVariableServiceGetVariablesKey,
  ...(queryKey ?? [{ limit, offset, orderBy }]),
];
export type XcomServiceGetXcomEntryDefaultResponse = Awaited<
  ReturnType<typeof XcomService.getXcomEntry>
>;
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
  ...(queryKey ?? [
    { dagId, dagRunId, deserialize, mapIndex, stringify, taskId, xcomKey },
  ]),
];
export type MonitorServiceGetHealthDefaultResponse = Awaited<
  ReturnType<typeof MonitorService.getHealth>
>;
export type MonitorServiceGetHealthQueryResult<
  TData = MonitorServiceGetHealthDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useMonitorServiceGetHealthKey = "MonitorServiceGetHealth";
export const UseMonitorServiceGetHealthKeyFn = (queryKey?: Array<unknown>) => [
  useMonitorServiceGetHealthKey,
  ...(queryKey ?? []),
];
export type VersionServiceGetVersionDefaultResponse = Awaited<
  ReturnType<typeof VersionService.getVersion>
>;
export type VersionServiceGetVersionQueryResult<
  TData = VersionServiceGetVersionDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useVersionServiceGetVersionKey = "VersionServiceGetVersion";
export const UseVersionServiceGetVersionKeyFn = (queryKey?: Array<unknown>) => [
  useVersionServiceGetVersionKey,
  ...(queryKey ?? []),
];
export type AssetServiceCreateAssetEventMutationResult = Awaited<
  ReturnType<typeof AssetService.createAssetEvent>
>;
export type BackfillServiceCreateBackfillMutationResult = Awaited<
  ReturnType<typeof BackfillService.createBackfill>
>;
export type ConnectionServicePostConnectionMutationResult = Awaited<
  ReturnType<typeof ConnectionService.postConnection>
>;
export type ConnectionServiceTestConnectionMutationResult = Awaited<
  ReturnType<typeof ConnectionService.testConnection>
>;
export type DagRunServiceClearDagRunMutationResult = Awaited<
  ReturnType<typeof DagRunService.clearDagRun>
>;
export type PoolServicePostPoolMutationResult = Awaited<
  ReturnType<typeof PoolService.postPool>
>;
export type PoolServicePostPoolsMutationResult = Awaited<
  ReturnType<typeof PoolService.postPools>
>;
export type TaskInstanceServiceGetTaskInstancesBatchMutationResult = Awaited<
  ReturnType<typeof TaskInstanceService.getTaskInstancesBatch>
>;
export type TaskInstanceServicePostClearTaskInstancesMutationResult = Awaited<
  ReturnType<typeof TaskInstanceService.postClearTaskInstances>
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
export type ConnectionServicePatchConnectionMutationResult = Awaited<
  ReturnType<typeof ConnectionService.patchConnection>
>;
export type DagRunServicePatchDagRunMutationResult = Awaited<
  ReturnType<typeof DagRunService.patchDagRun>
>;
export type DagServicePatchDagsMutationResult = Awaited<
  ReturnType<typeof DagService.patchDags>
>;
export type DagServicePatchDagMutationResult = Awaited<
  ReturnType<typeof DagService.patchDag>
>;
export type PoolServicePatchPoolMutationResult = Awaited<
  ReturnType<typeof PoolService.patchPool>
>;
export type VariableServicePatchVariableMutationResult = Awaited<
  ReturnType<typeof VariableService.patchVariable>
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
export type DagRunServiceDeleteDagRunMutationResult = Awaited<
  ReturnType<typeof DagRunService.deleteDagRun>
>;
export type DagServiceDeleteDagMutationResult = Awaited<
  ReturnType<typeof DagService.deleteDag>
>;
export type PoolServiceDeletePoolMutationResult = Awaited<
  ReturnType<typeof PoolService.deletePool>
>;
export type VariableServiceDeleteVariableMutationResult = Awaited<
  ReturnType<typeof VariableService.deleteVariable>
>;
