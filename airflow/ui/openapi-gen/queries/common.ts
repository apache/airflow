// generated with @7nohe/openapi-react-query-codegen@1.6.0
import { UseQueryResult } from "@tanstack/react-query";

import {
  ConfigService,
  ConnectionService,
  DagRunService,
  DagService,
  DagStatsService,
  DagWarningService,
  DatasetService,
  EventLogService,
  ImportErrorService,
  MonitoringService,
  PermissionService,
  PluginService,
  PoolService,
  ProviderService,
  RoleService,
  TaskInstanceService,
  UserService,
  VariableService,
  XcomService,
} from "../requests/services.gen";

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
    dagIdPattern,
    fields,
    limit,
    offset,
    onlyActive,
    orderBy,
    paused,
    tags,
  }: {
    dagIdPattern?: string;
    fields?: string[];
    limit?: number;
    offset?: number;
    onlyActive?: boolean;
    orderBy?: string;
    paused?: boolean;
    tags?: string[];
  } = {},
  queryKey?: Array<unknown>,
) => [
  useDagServiceGetDagsKey,
  ...(queryKey ?? [
    { dagIdPattern, fields, limit, offset, onlyActive, orderBy, paused, tags },
  ]),
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
    fields,
  }: {
    dagId: string;
    fields?: string[];
  },
  queryKey?: Array<unknown>,
) => [useDagServiceGetDagKey, ...(queryKey ?? [{ dagId, fields }])];
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
    fields,
  }: {
    dagId: string;
    fields?: string[];
  },
  queryKey?: Array<unknown>,
) => [useDagServiceGetDagDetailsKey, ...(queryKey ?? [{ dagId, fields }])];
export type DagServiceGetTasksDefaultResponse = Awaited<
  ReturnType<typeof DagService.getTasks>
>;
export type DagServiceGetTasksQueryResult<
  TData = DagServiceGetTasksDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDagServiceGetTasksKey = "DagServiceGetTasks";
export const UseDagServiceGetTasksKeyFn = (
  {
    dagId,
    orderBy,
  }: {
    dagId: string;
    orderBy?: string;
  },
  queryKey?: Array<unknown>,
) => [useDagServiceGetTasksKey, ...(queryKey ?? [{ dagId, orderBy }])];
export type DagServiceGetTaskDefaultResponse = Awaited<
  ReturnType<typeof DagService.getTask>
>;
export type DagServiceGetTaskQueryResult<
  TData = DagServiceGetTaskDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDagServiceGetTaskKey = "DagServiceGetTask";
export const UseDagServiceGetTaskKeyFn = (
  {
    dagId,
    taskId,
  }: {
    dagId: string;
    taskId: string;
  },
  queryKey?: Array<unknown>,
) => [useDagServiceGetTaskKey, ...(queryKey ?? [{ dagId, taskId }])];
export type DagServiceGetDagSourceDefaultResponse = Awaited<
  ReturnType<typeof DagService.getDagSource>
>;
export type DagServiceGetDagSourceQueryResult<
  TData = DagServiceGetDagSourceDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDagServiceGetDagSourceKey = "DagServiceGetDagSource";
export const UseDagServiceGetDagSourceKeyFn = (
  {
    fileToken,
  }: {
    fileToken: string;
  },
  queryKey?: Array<unknown>,
) => [useDagServiceGetDagSourceKey, ...(queryKey ?? [{ fileToken }])];
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
    taskId,
  }: {
    dagId: string;
    dagRunId: string;
    taskId: string;
  },
  queryKey?: Array<unknown>,
) => [
  useTaskInstanceServiceGetTaskInstanceDependenciesKey,
  ...(queryKey ?? [{ dagId, dagRunId, taskId }]),
];
export type TaskInstanceServiceGetMappedTaskInstanceDependenciesDefaultResponse =
  Awaited<
    ReturnType<typeof TaskInstanceService.getMappedTaskInstanceDependencies>
  >;
export type TaskInstanceServiceGetMappedTaskInstanceDependenciesQueryResult<
  TData = TaskInstanceServiceGetMappedTaskInstanceDependenciesDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useTaskInstanceServiceGetMappedTaskInstanceDependenciesKey =
  "TaskInstanceServiceGetMappedTaskInstanceDependencies";
export const UseTaskInstanceServiceGetMappedTaskInstanceDependenciesKeyFn = (
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
  useTaskInstanceServiceGetMappedTaskInstanceDependenciesKey,
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
    executionDateGte,
    executionDateLte,
    executor,
    limit,
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
    executionDateGte?: string;
    executionDateLte?: string;
    executor?: string[];
    limit?: number;
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
      executionDateGte,
      executionDateLte,
      executor,
      limit,
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
    executionDateGte,
    executionDateLte,
    executor,
    limit,
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
    executionDateGte?: string;
    executionDateLte?: string;
    executor?: string[];
    limit?: number;
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
      executionDateGte,
      executionDateLte,
      executor,
      limit,
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
    taskId,
    taskTryNumber,
  }: {
    dagId: string;
    dagRunId: string;
    taskId: string;
    taskTryNumber: number;
  },
  queryKey?: Array<unknown>,
) => [
  useTaskInstanceServiceGetTaskInstanceTryDetailsKey,
  ...(queryKey ?? [{ dagId, dagRunId, taskId, taskTryNumber }]),
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
    limit,
    offset,
    orderBy,
    taskId,
  }: {
    dagId: string;
    dagRunId: string;
    limit?: number;
    offset?: number;
    orderBy?: string;
    taskId: string;
  },
  queryKey?: Array<unknown>,
) => [
  useTaskInstanceServiceGetTaskInstanceTriesKey,
  ...(queryKey ?? [{ dagId, dagRunId, limit, offset, orderBy, taskId }]),
];
export type TaskInstanceServiceGetMappedTaskInstanceTriesDefaultResponse =
  Awaited<ReturnType<typeof TaskInstanceService.getMappedTaskInstanceTries>>;
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
    limit,
    mapIndex,
    offset,
    orderBy,
    taskId,
  }: {
    dagId: string;
    dagRunId: string;
    limit?: number;
    mapIndex: number;
    offset?: number;
    orderBy?: string;
    taskId: string;
  },
  queryKey?: Array<unknown>,
) => [
  useTaskInstanceServiceGetMappedTaskInstanceTriesKey,
  ...(queryKey ?? [
    { dagId, dagRunId, limit, mapIndex, offset, orderBy, taskId },
  ]),
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
export type TaskInstanceServiceGetExtraLinksDefaultResponse = Awaited<
  ReturnType<typeof TaskInstanceService.getExtraLinks>
>;
export type TaskInstanceServiceGetExtraLinksQueryResult<
  TData = TaskInstanceServiceGetExtraLinksDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useTaskInstanceServiceGetExtraLinksKey =
  "TaskInstanceServiceGetExtraLinks";
export const UseTaskInstanceServiceGetExtraLinksKeyFn = (
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
  useTaskInstanceServiceGetExtraLinksKey,
  ...(queryKey ?? [{ dagId, dagRunId, taskId }]),
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
    dagId,
    dagRunId,
    fullContent,
    mapIndex,
    taskId,
    taskTryNumber,
    token,
  }: {
    dagId: string;
    dagRunId: string;
    fullContent?: boolean;
    mapIndex?: number;
    taskId: string;
    taskTryNumber: number;
    token?: string;
  },
  queryKey?: Array<unknown>,
) => [
  useTaskInstanceServiceGetLogKey,
  ...(queryKey ?? [
    { dagId, dagRunId, fullContent, mapIndex, taskId, taskTryNumber, token },
  ]),
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
    executionDateGte,
    executionDateLte,
    fields,
    limit,
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
    executionDateGte?: string;
    executionDateLte?: string;
    fields?: string[];
    limit?: number;
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
      executionDateGte,
      executionDateLte,
      fields,
      limit,
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
    fields,
  }: {
    dagId: string;
    dagRunId: string;
    fields?: string[];
  },
  queryKey?: Array<unknown>,
) => [
  useDagRunServiceGetDagRunKey,
  ...(queryKey ?? [{ dagId, dagRunId, fields }]),
];
export type DagRunServiceGetUpstreamDatasetEventsDefaultResponse = Awaited<
  ReturnType<typeof DagRunService.getUpstreamDatasetEvents>
>;
export type DagRunServiceGetUpstreamDatasetEventsQueryResult<
  TData = DagRunServiceGetUpstreamDatasetEventsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDagRunServiceGetUpstreamDatasetEventsKey =
  "DagRunServiceGetUpstreamDatasetEvents";
export const UseDagRunServiceGetUpstreamDatasetEventsKeyFn = (
  {
    dagId,
    dagRunId,
  }: {
    dagId: string;
    dagRunId: string;
  },
  queryKey?: Array<unknown>,
) => [
  useDagRunServiceGetUpstreamDatasetEventsKey,
  ...(queryKey ?? [{ dagId, dagRunId }]),
];
export type DatasetServiceGetUpstreamDatasetEventsDefaultResponse = Awaited<
  ReturnType<typeof DatasetService.getUpstreamDatasetEvents>
>;
export type DatasetServiceGetUpstreamDatasetEventsQueryResult<
  TData = DatasetServiceGetUpstreamDatasetEventsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDatasetServiceGetUpstreamDatasetEventsKey =
  "DatasetServiceGetUpstreamDatasetEvents";
export const UseDatasetServiceGetUpstreamDatasetEventsKeyFn = (
  {
    dagId,
    dagRunId,
  }: {
    dagId: string;
    dagRunId: string;
  },
  queryKey?: Array<unknown>,
) => [
  useDatasetServiceGetUpstreamDatasetEventsKey,
  ...(queryKey ?? [{ dagId, dagRunId }]),
];
export type DatasetServiceGetDagDatasetQueuedEventDefaultResponse = Awaited<
  ReturnType<typeof DatasetService.getDagDatasetQueuedEvent>
>;
export type DatasetServiceGetDagDatasetQueuedEventQueryResult<
  TData = DatasetServiceGetDagDatasetQueuedEventDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDatasetServiceGetDagDatasetQueuedEventKey =
  "DatasetServiceGetDagDatasetQueuedEvent";
export const UseDatasetServiceGetDagDatasetQueuedEventKeyFn = (
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
  useDatasetServiceGetDagDatasetQueuedEventKey,
  ...(queryKey ?? [{ before, dagId, uri }]),
];
export type DatasetServiceGetDagDatasetQueuedEventsDefaultResponse = Awaited<
  ReturnType<typeof DatasetService.getDagDatasetQueuedEvents>
>;
export type DatasetServiceGetDagDatasetQueuedEventsQueryResult<
  TData = DatasetServiceGetDagDatasetQueuedEventsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDatasetServiceGetDagDatasetQueuedEventsKey =
  "DatasetServiceGetDagDatasetQueuedEvents";
export const UseDatasetServiceGetDagDatasetQueuedEventsKeyFn = (
  {
    before,
    dagId,
  }: {
    before?: string;
    dagId: string;
  },
  queryKey?: Array<unknown>,
) => [
  useDatasetServiceGetDagDatasetQueuedEventsKey,
  ...(queryKey ?? [{ before, dagId }]),
];
export type DatasetServiceGetDatasetQueuedEventsDefaultResponse = Awaited<
  ReturnType<typeof DatasetService.getDatasetQueuedEvents>
>;
export type DatasetServiceGetDatasetQueuedEventsQueryResult<
  TData = DatasetServiceGetDatasetQueuedEventsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDatasetServiceGetDatasetQueuedEventsKey =
  "DatasetServiceGetDatasetQueuedEvents";
export const UseDatasetServiceGetDatasetQueuedEventsKeyFn = (
  {
    before,
    uri,
  }: {
    before?: string;
    uri: string;
  },
  queryKey?: Array<unknown>,
) => [
  useDatasetServiceGetDatasetQueuedEventsKey,
  ...(queryKey ?? [{ before, uri }]),
];
export type DatasetServiceGetDatasetsDefaultResponse = Awaited<
  ReturnType<typeof DatasetService.getDatasets>
>;
export type DatasetServiceGetDatasetsQueryResult<
  TData = DatasetServiceGetDatasetsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDatasetServiceGetDatasetsKey = "DatasetServiceGetDatasets";
export const UseDatasetServiceGetDatasetsKeyFn = (
  {
    dagIds,
    limit,
    offset,
    orderBy,
    uriPattern,
  }: {
    dagIds?: string;
    limit?: number;
    offset?: number;
    orderBy?: string;
    uriPattern?: string;
  } = {},
  queryKey?: Array<unknown>,
) => [
  useDatasetServiceGetDatasetsKey,
  ...(queryKey ?? [{ dagIds, limit, offset, orderBy, uriPattern }]),
];
export type DatasetServiceGetDatasetDefaultResponse = Awaited<
  ReturnType<typeof DatasetService.getDataset>
>;
export type DatasetServiceGetDatasetQueryResult<
  TData = DatasetServiceGetDatasetDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDatasetServiceGetDatasetKey = "DatasetServiceGetDataset";
export const UseDatasetServiceGetDatasetKeyFn = (
  {
    uri,
  }: {
    uri: string;
  },
  queryKey?: Array<unknown>,
) => [useDatasetServiceGetDatasetKey, ...(queryKey ?? [{ uri }])];
export type DatasetServiceGetDatasetEventsDefaultResponse = Awaited<
  ReturnType<typeof DatasetService.getDatasetEvents>
>;
export type DatasetServiceGetDatasetEventsQueryResult<
  TData = DatasetServiceGetDatasetEventsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDatasetServiceGetDatasetEventsKey =
  "DatasetServiceGetDatasetEvents";
export const UseDatasetServiceGetDatasetEventsKeyFn = (
  {
    datasetId,
    limit,
    offset,
    orderBy,
    sourceDagId,
    sourceMapIndex,
    sourceRunId,
    sourceTaskId,
  }: {
    datasetId?: number;
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
  useDatasetServiceGetDatasetEventsKey,
  ...(queryKey ?? [
    {
      datasetId,
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
    excludedEvents?: string;
    includedEvents?: string;
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
export type ProviderServiceGetProvidersDefaultResponse = Awaited<
  ReturnType<typeof ProviderService.getProviders>
>;
export type ProviderServiceGetProvidersQueryResult<
  TData = ProviderServiceGetProvidersDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useProviderServiceGetProvidersKey = "ProviderServiceGetProviders";
export const UseProviderServiceGetProvidersKeyFn = (
  queryKey?: Array<unknown>,
) => [useProviderServiceGetProvidersKey, ...(queryKey ?? [])];
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
export type XcomServiceGetXcomEntriesDefaultResponse = Awaited<
  ReturnType<typeof XcomService.getXcomEntries>
>;
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
  ...(queryKey ?? [
    { dagId, dagRunId, limit, mapIndex, offset, taskId, xcomKey },
  ]),
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
    dagIds: string;
  },
  queryKey?: Array<unknown>,
) => [useDagStatsServiceGetDagStatsKey, ...(queryKey ?? [{ dagIds }])];
export type DagWarningServiceGetDagWarningsDefaultResponse = Awaited<
  ReturnType<typeof DagWarningService.getDagWarnings>
>;
export type DagWarningServiceGetDagWarningsQueryResult<
  TData = DagWarningServiceGetDagWarningsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDagWarningServiceGetDagWarningsKey =
  "DagWarningServiceGetDagWarnings";
export const UseDagWarningServiceGetDagWarningsKeyFn = (
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
    warningType?: string;
  } = {},
  queryKey?: Array<unknown>,
) => [
  useDagWarningServiceGetDagWarningsKey,
  ...(queryKey ?? [{ dagId, limit, offset, orderBy, warningType }]),
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
    section,
  }: {
    section?: string;
  } = {},
  queryKey?: Array<unknown>,
) => [useConfigServiceGetConfigKey, ...(queryKey ?? [{ section }])];
export type ConfigServiceGetValueDefaultResponse = Awaited<
  ReturnType<typeof ConfigService.getValue>
>;
export type ConfigServiceGetValueQueryResult<
  TData = ConfigServiceGetValueDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useConfigServiceGetValueKey = "ConfigServiceGetValue";
export const UseConfigServiceGetValueKeyFn = (
  {
    option,
    section,
  }: {
    option: string;
    section: string;
  },
  queryKey?: Array<unknown>,
) => [useConfigServiceGetValueKey, ...(queryKey ?? [{ option, section }])];
export type MonitoringServiceGetHealthDefaultResponse = Awaited<
  ReturnType<typeof MonitoringService.getHealth>
>;
export type MonitoringServiceGetHealthQueryResult<
  TData = MonitoringServiceGetHealthDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useMonitoringServiceGetHealthKey = "MonitoringServiceGetHealth";
export const UseMonitoringServiceGetHealthKeyFn = (
  queryKey?: Array<unknown>,
) => [useMonitoringServiceGetHealthKey, ...(queryKey ?? [])];
export type MonitoringServiceGetVersionDefaultResponse = Awaited<
  ReturnType<typeof MonitoringService.getVersion>
>;
export type MonitoringServiceGetVersionQueryResult<
  TData = MonitoringServiceGetVersionDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useMonitoringServiceGetVersionKey = "MonitoringServiceGetVersion";
export const UseMonitoringServiceGetVersionKeyFn = (
  queryKey?: Array<unknown>,
) => [useMonitoringServiceGetVersionKey, ...(queryKey ?? [])];
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
export type RoleServiceGetRolesDefaultResponse = Awaited<
  ReturnType<typeof RoleService.getRoles>
>;
export type RoleServiceGetRolesQueryResult<
  TData = RoleServiceGetRolesDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useRoleServiceGetRolesKey = "RoleServiceGetRoles";
export const UseRoleServiceGetRolesKeyFn = (
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
) => [useRoleServiceGetRolesKey, ...(queryKey ?? [{ limit, offset, orderBy }])];
export type RoleServiceGetRoleDefaultResponse = Awaited<
  ReturnType<typeof RoleService.getRole>
>;
export type RoleServiceGetRoleQueryResult<
  TData = RoleServiceGetRoleDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useRoleServiceGetRoleKey = "RoleServiceGetRole";
export const UseRoleServiceGetRoleKeyFn = (
  {
    roleName,
  }: {
    roleName: string;
  },
  queryKey?: Array<unknown>,
) => [useRoleServiceGetRoleKey, ...(queryKey ?? [{ roleName }])];
export type PermissionServiceGetPermissionsDefaultResponse = Awaited<
  ReturnType<typeof PermissionService.getPermissions>
>;
export type PermissionServiceGetPermissionsQueryResult<
  TData = PermissionServiceGetPermissionsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const usePermissionServiceGetPermissionsKey =
  "PermissionServiceGetPermissions";
export const UsePermissionServiceGetPermissionsKeyFn = (
  {
    limit,
    offset,
  }: {
    limit?: number;
    offset?: number;
  } = {},
  queryKey?: Array<unknown>,
) => [
  usePermissionServiceGetPermissionsKey,
  ...(queryKey ?? [{ limit, offset }]),
];
export type UserServiceGetUsersDefaultResponse = Awaited<
  ReturnType<typeof UserService.getUsers>
>;
export type UserServiceGetUsersQueryResult<
  TData = UserServiceGetUsersDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useUserServiceGetUsersKey = "UserServiceGetUsers";
export const UseUserServiceGetUsersKeyFn = (
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
) => [useUserServiceGetUsersKey, ...(queryKey ?? [{ limit, offset, orderBy }])];
export type UserServiceGetUserDefaultResponse = Awaited<
  ReturnType<typeof UserService.getUser>
>;
export type UserServiceGetUserQueryResult<
  TData = UserServiceGetUserDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useUserServiceGetUserKey = "UserServiceGetUser";
export const UseUserServiceGetUserKeyFn = (
  {
    username,
  }: {
    username: string;
  },
  queryKey?: Array<unknown>,
) => [useUserServiceGetUserKey, ...(queryKey ?? [{ username }])];
export type ConnectionServicePostConnectionMutationResult = Awaited<
  ReturnType<typeof ConnectionService.postConnection>
>;
export type ConnectionServiceTestConnectionMutationResult = Awaited<
  ReturnType<typeof ConnectionService.testConnection>
>;
export type DagServicePostClearTaskInstancesMutationResult = Awaited<
  ReturnType<typeof DagService.postClearTaskInstances>
>;
export type DagServicePostSetTaskInstancesStateMutationResult = Awaited<
  ReturnType<typeof DagService.postSetTaskInstancesState>
>;
export type TaskInstanceServiceGetTaskInstancesBatchMutationResult = Awaited<
  ReturnType<typeof TaskInstanceService.getTaskInstancesBatch>
>;
export type DagRunServicePostDagRunMutationResult = Awaited<
  ReturnType<typeof DagRunService.postDagRun>
>;
export type DagRunServiceGetDagRunsBatchMutationResult = Awaited<
  ReturnType<typeof DagRunService.getDagRunsBatch>
>;
export type DagRunServiceClearDagRunMutationResult = Awaited<
  ReturnType<typeof DagRunService.clearDagRun>
>;
export type DatasetServiceCreateDatasetEventMutationResult = Awaited<
  ReturnType<typeof DatasetService.createDatasetEvent>
>;
export type PoolServicePostPoolMutationResult = Awaited<
  ReturnType<typeof PoolService.postPool>
>;
export type VariableServicePostVariablesMutationResult = Awaited<
  ReturnType<typeof VariableService.postVariables>
>;
export type RoleServicePostRoleMutationResult = Awaited<
  ReturnType<typeof RoleService.postRole>
>;
export type UserServicePostUserMutationResult = Awaited<
  ReturnType<typeof UserService.postUser>
>;
export type DagServiceReparseDagFileMutationResult = Awaited<
  ReturnType<typeof DagService.reparseDagFile>
>;
export type ConnectionServicePatchConnectionMutationResult = Awaited<
  ReturnType<typeof ConnectionService.patchConnection>
>;
export type DagServicePatchDagsMutationResult = Awaited<
  ReturnType<typeof DagService.patchDags>
>;
export type DagServicePatchDagMutationResult = Awaited<
  ReturnType<typeof DagService.patchDag>
>;
export type TaskInstanceServiceSetTaskInstanceNoteMutationResult = Awaited<
  ReturnType<typeof TaskInstanceService.setTaskInstanceNote>
>;
export type TaskInstanceServiceSetMappedTaskInstanceNoteMutationResult =
  Awaited<ReturnType<typeof TaskInstanceService.setMappedTaskInstanceNote>>;
export type TaskInstanceServicePatchTaskInstanceMutationResult = Awaited<
  ReturnType<typeof TaskInstanceService.patchTaskInstance>
>;
export type TaskInstanceServicePatchMappedTaskInstanceMutationResult = Awaited<
  ReturnType<typeof TaskInstanceService.patchMappedTaskInstance>
>;
export type DagRunServiceUpdateDagRunStateMutationResult = Awaited<
  ReturnType<typeof DagRunService.updateDagRunState>
>;
export type DagRunServiceSetDagRunNoteMutationResult = Awaited<
  ReturnType<typeof DagRunService.setDagRunNote>
>;
export type PoolServicePatchPoolMutationResult = Awaited<
  ReturnType<typeof PoolService.patchPool>
>;
export type VariableServicePatchVariableMutationResult = Awaited<
  ReturnType<typeof VariableService.patchVariable>
>;
export type RoleServicePatchRoleMutationResult = Awaited<
  ReturnType<typeof RoleService.patchRole>
>;
export type UserServicePatchUserMutationResult = Awaited<
  ReturnType<typeof UserService.patchUser>
>;
export type ConnectionServiceDeleteConnectionMutationResult = Awaited<
  ReturnType<typeof ConnectionService.deleteConnection>
>;
export type DagServiceDeleteDagMutationResult = Awaited<
  ReturnType<typeof DagService.deleteDag>
>;
export type DagRunServiceDeleteDagRunMutationResult = Awaited<
  ReturnType<typeof DagRunService.deleteDagRun>
>;
export type DatasetServiceDeleteDagDatasetQueuedEventMutationResult = Awaited<
  ReturnType<typeof DatasetService.deleteDagDatasetQueuedEvent>
>;
export type DatasetServiceDeleteDagDatasetQueuedEventsMutationResult = Awaited<
  ReturnType<typeof DatasetService.deleteDagDatasetQueuedEvents>
>;
export type DatasetServiceDeleteDatasetQueuedEventsMutationResult = Awaited<
  ReturnType<typeof DatasetService.deleteDatasetQueuedEvents>
>;
export type PoolServiceDeletePoolMutationResult = Awaited<
  ReturnType<typeof PoolService.deletePool>
>;
export type VariableServiceDeleteVariableMutationResult = Awaited<
  ReturnType<typeof VariableService.deleteVariable>
>;
export type RoleServiceDeleteRoleMutationResult = Awaited<
  ReturnType<typeof RoleService.deleteRole>
>;
export type UserServiceDeleteUserMutationResult = Awaited<
  ReturnType<typeof UserService.deleteUser>
>;
