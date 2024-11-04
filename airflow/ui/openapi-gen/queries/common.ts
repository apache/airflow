// generated with @7nohe/openapi-react-query-codegen@1.6.0
import { UseQueryResult } from "@tanstack/react-query";

import {
  AssetService,
  BackfillService,
  ConnectionService,
  DagRunService,
  DagService,
  DagSourceService,
  DagWarningService,
  DagsService,
  DashboardService,
  EventLogService,
  MonitorService,
  PluginService,
  PoolService,
  ProviderService,
  TaskInstanceService,
  VariableService,
  VersionService,
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
    endDate: string;
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
    fileToken,
  }: {
    accept?: string;
    fileToken: string;
  },
  queryKey?: Array<unknown>,
) => [
  useDagSourceServiceGetDagSourceKey,
  ...(queryKey ?? [{ accept, fileToken }]),
];
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
export type BackfillServiceCreateBackfillMutationResult = Awaited<
  ReturnType<typeof BackfillService.createBackfill>
>;
export type PoolServicePostPoolMutationResult = Awaited<
  ReturnType<typeof PoolService.postPool>
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
export type DagServicePatchDagsMutationResult = Awaited<
  ReturnType<typeof DagService.patchDags>
>;
export type DagServicePatchDagMutationResult = Awaited<
  ReturnType<typeof DagService.patchDag>
>;
export type DagRunServicePatchDagRunStateMutationResult = Awaited<
  ReturnType<typeof DagRunService.patchDagRunState>
>;
export type PoolServicePatchPoolMutationResult = Awaited<
  ReturnType<typeof PoolService.patchPool>
>;
export type VariableServicePatchVariableMutationResult = Awaited<
  ReturnType<typeof VariableService.patchVariable>
>;
export type DagServiceDeleteDagMutationResult = Awaited<
  ReturnType<typeof DagService.deleteDag>
>;
export type ConnectionServiceDeleteConnectionMutationResult = Awaited<
  ReturnType<typeof ConnectionService.deleteConnection>
>;
export type DagRunServiceDeleteDagRunMutationResult = Awaited<
  ReturnType<typeof DagRunService.deleteDagRun>
>;
export type PoolServiceDeletePoolMutationResult = Awaited<
  ReturnType<typeof PoolService.deletePool>
>;
export type VariableServiceDeleteVariableMutationResult = Awaited<
  ReturnType<typeof VariableService.deleteVariable>
>;
