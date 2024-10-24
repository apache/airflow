// generated with @7nohe/openapi-react-query-codegen@1.6.0
import { UseQueryResult } from "@tanstack/react-query";

import {
  AssetService,
  ConnectionService,
  DagRunService,
  DagService,
  DashboardService,
  MonitorService,
  PluginService,
  PoolService,
  ProviderService,
  VariableService,
  VersionService,
} from "../requests/services.gen";
import { DagRunState } from "../requests/types.gen";

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
export type VariableServicePostVariableMutationResult = Awaited<
  ReturnType<typeof VariableService.postVariable>
>;
export type DagServicePatchDagsMutationResult = Awaited<
  ReturnType<typeof DagService.patchDags>
>;
export type DagServicePatchDagMutationResult = Awaited<
  ReturnType<typeof DagService.patchDag>
>;
export type VariableServicePatchVariableMutationResult = Awaited<
  ReturnType<typeof VariableService.patchVariable>
>;
export type PoolServicePatchPoolMutationResult = Awaited<
  ReturnType<typeof PoolService.patchPool>
>;
export type DagServiceDeleteDagMutationResult = Awaited<
  ReturnType<typeof DagService.deleteDag>
>;
export type ConnectionServiceDeleteConnectionMutationResult = Awaited<
  ReturnType<typeof ConnectionService.deleteConnection>
>;
export type VariableServiceDeleteVariableMutationResult = Awaited<
  ReturnType<typeof VariableService.deleteVariable>
>;
export type DagRunServiceDeleteDagRunMutationResult = Awaited<
  ReturnType<typeof DagRunService.deleteDagRun>
>;
export type PoolServiceDeletePoolMutationResult = Awaited<
  ReturnType<typeof PoolService.deletePool>
>;
