// generated with @7nohe/openapi-react-query-codegen@1.6.0

import {
  UseMutationOptions,
  UseQueryOptions,
  useMutation,
  useQuery,
} from "@tanstack/react-query";
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
import {
  ClearDagRun,
  ClearTaskInstances,
  Connection,
  CreateDatasetEvent,
  DAG,
  DAGRun,
  ListDagRunsForm,
  ListTaskInstanceForm,
  Pool,
  Role,
  SetDagRunNote,
  SetTaskInstanceNote,
  UpdateDagRunState,
  UpdateTaskInstance,
  UpdateTaskInstancesState,
  User,
  Variable,
} from "../requests/types.gen";
import * as Common from "./common";
export const useConnectionServiceGetConnections = <
  TData = Common.ConnectionServiceGetConnectionsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    limit,
    offset,
    orderBy,
  }: {
    limit?: number;
    offset?: number;
    orderBy?: string;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseConnectionServiceGetConnectionsKeyFn(
      { limit, offset, orderBy },
      queryKey
    ),
    queryFn: () =>
      ConnectionService.getConnections({ limit, offset, orderBy }) as TData,
    ...options,
  });
export const useConnectionServiceGetConnection = <
  TData = Common.ConnectionServiceGetConnectionDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    connectionId,
  }: {
    connectionId: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseConnectionServiceGetConnectionKeyFn(
      { connectionId },
      queryKey
    ),
    queryFn: () => ConnectionService.getConnection({ connectionId }) as TData,
    ...options,
  });
export const useDagServiceGetDags = <
  TData = Common.DagServiceGetDagsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
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
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDagServiceGetDagsKeyFn(
      {
        dagIdPattern,
        fields,
        limit,
        offset,
        onlyActive,
        orderBy,
        paused,
        tags,
      },
      queryKey
    ),
    queryFn: () =>
      DagService.getDags({
        dagIdPattern,
        fields,
        limit,
        offset,
        onlyActive,
        orderBy,
        paused,
        tags,
      }) as TData,
    ...options,
  });
export const useDagServiceGetDag = <
  TData = Common.DagServiceGetDagDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    fields,
  }: {
    dagId: string;
    fields?: string[];
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDagServiceGetDagKeyFn({ dagId, fields }, queryKey),
    queryFn: () => DagService.getDag({ dagId, fields }) as TData,
    ...options,
  });
export const useDagServiceGetDagDetails = <
  TData = Common.DagServiceGetDagDetailsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    fields,
  }: {
    dagId: string;
    fields?: string[];
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDagServiceGetDagDetailsKeyFn(
      { dagId, fields },
      queryKey
    ),
    queryFn: () => DagService.getDagDetails({ dagId, fields }) as TData,
    ...options,
  });
export const useDagServiceGetTasks = <
  TData = Common.DagServiceGetTasksDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    orderBy,
  }: {
    dagId: string;
    orderBy?: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDagServiceGetTasksKeyFn({ dagId, orderBy }, queryKey),
    queryFn: () => DagService.getTasks({ dagId, orderBy }) as TData,
    ...options,
  });
export const useDagServiceGetTask = <
  TData = Common.DagServiceGetTaskDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    taskId,
  }: {
    dagId: string;
    taskId: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDagServiceGetTaskKeyFn({ dagId, taskId }, queryKey),
    queryFn: () => DagService.getTask({ dagId, taskId }) as TData,
    ...options,
  });
export const useDagServiceGetDagSource = <
  TData = Common.DagServiceGetDagSourceDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    fileToken,
  }: {
    fileToken: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDagServiceGetDagSourceKeyFn({ fileToken }, queryKey),
    queryFn: () => DagService.getDagSource({ fileToken }) as TData,
    ...options,
  });
export const useTaskInstanceServiceGetTaskInstanceDependencies = <
  TData = Common.TaskInstanceServiceGetTaskInstanceDependenciesDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    dagRunId,
    taskId,
  }: {
    dagId: string;
    dagRunId: string;
    taskId: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseTaskInstanceServiceGetTaskInstanceDependenciesKeyFn(
      { dagId, dagRunId, taskId },
      queryKey
    ),
    queryFn: () =>
      TaskInstanceService.getTaskInstanceDependencies({
        dagId,
        dagRunId,
        taskId,
      }) as TData,
    ...options,
  });
export const useTaskInstanceServiceGetMappedTaskInstanceDependencies = <
  TData = Common.TaskInstanceServiceGetMappedTaskInstanceDependenciesDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
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
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey:
      Common.UseTaskInstanceServiceGetMappedTaskInstanceDependenciesKeyFn(
        { dagId, dagRunId, mapIndex, taskId },
        queryKey
      ),
    queryFn: () =>
      TaskInstanceService.getMappedTaskInstanceDependencies({
        dagId,
        dagRunId,
        mapIndex,
        taskId,
      }) as TData,
    ...options,
  });
export const useTaskInstanceServiceGetTaskInstances = <
  TData = Common.TaskInstanceServiceGetTaskInstancesDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
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
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseTaskInstanceServiceGetTaskInstancesKeyFn(
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
      queryKey
    ),
    queryFn: () =>
      TaskInstanceService.getTaskInstances({
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
      }) as TData,
    ...options,
  });
export const useTaskInstanceServiceGetTaskInstance = <
  TData = Common.TaskInstanceServiceGetTaskInstanceDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    dagRunId,
    taskId,
  }: {
    dagId: string;
    dagRunId: string;
    taskId: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseTaskInstanceServiceGetTaskInstanceKeyFn(
      { dagId, dagRunId, taskId },
      queryKey
    ),
    queryFn: () =>
      TaskInstanceService.getTaskInstance({ dagId, dagRunId, taskId }) as TData,
    ...options,
  });
export const useTaskInstanceServiceGetMappedTaskInstance = <
  TData = Common.TaskInstanceServiceGetMappedTaskInstanceDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
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
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstanceKeyFn(
      { dagId, dagRunId, mapIndex, taskId },
      queryKey
    ),
    queryFn: () =>
      TaskInstanceService.getMappedTaskInstance({
        dagId,
        dagRunId,
        mapIndex,
        taskId,
      }) as TData,
    ...options,
  });
export const useTaskInstanceServiceGetMappedTaskInstances = <
  TData = Common.TaskInstanceServiceGetMappedTaskInstancesDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
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
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstancesKeyFn(
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
      queryKey
    ),
    queryFn: () =>
      TaskInstanceService.getMappedTaskInstances({
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
      }) as TData,
    ...options,
  });
export const useTaskInstanceServiceGetTaskInstanceTryDetails = <
  TData = Common.TaskInstanceServiceGetTaskInstanceTryDetailsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
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
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseTaskInstanceServiceGetTaskInstanceTryDetailsKeyFn(
      { dagId, dagRunId, taskId, taskTryNumber },
      queryKey
    ),
    queryFn: () =>
      TaskInstanceService.getTaskInstanceTryDetails({
        dagId,
        dagRunId,
        taskId,
        taskTryNumber,
      }) as TData,
    ...options,
  });
export const useTaskInstanceServiceGetTaskInstanceTries = <
  TData = Common.TaskInstanceServiceGetTaskInstanceTriesDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
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
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseTaskInstanceServiceGetTaskInstanceTriesKeyFn(
      { dagId, dagRunId, limit, offset, orderBy, taskId },
      queryKey
    ),
    queryFn: () =>
      TaskInstanceService.getTaskInstanceTries({
        dagId,
        dagRunId,
        limit,
        offset,
        orderBy,
        taskId,
      }) as TData,
    ...options,
  });
export const useTaskInstanceServiceGetMappedTaskInstanceTries = <
  TData = Common.TaskInstanceServiceGetMappedTaskInstanceTriesDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
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
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstanceTriesKeyFn(
      { dagId, dagRunId, limit, mapIndex, offset, orderBy, taskId },
      queryKey
    ),
    queryFn: () =>
      TaskInstanceService.getMappedTaskInstanceTries({
        dagId,
        dagRunId,
        limit,
        mapIndex,
        offset,
        orderBy,
        taskId,
      }) as TData,
    ...options,
  });
export const useTaskInstanceServiceGetMappedTaskInstanceTryDetails = <
  TData = Common.TaskInstanceServiceGetMappedTaskInstanceTryDetailsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
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
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstanceTryDetailsKeyFn(
      { dagId, dagRunId, mapIndex, taskId, taskTryNumber },
      queryKey
    ),
    queryFn: () =>
      TaskInstanceService.getMappedTaskInstanceTryDetails({
        dagId,
        dagRunId,
        mapIndex,
        taskId,
        taskTryNumber,
      }) as TData,
    ...options,
  });
export const useTaskInstanceServiceGetExtraLinks = <
  TData = Common.TaskInstanceServiceGetExtraLinksDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    dagRunId,
    taskId,
  }: {
    dagId: string;
    dagRunId: string;
    taskId: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseTaskInstanceServiceGetExtraLinksKeyFn(
      { dagId, dagRunId, taskId },
      queryKey
    ),
    queryFn: () =>
      TaskInstanceService.getExtraLinks({ dagId, dagRunId, taskId }) as TData,
    ...options,
  });
export const useTaskInstanceServiceGetLog = <
  TData = Common.TaskInstanceServiceGetLogDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
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
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseTaskInstanceServiceGetLogKeyFn(
      { dagId, dagRunId, fullContent, mapIndex, taskId, taskTryNumber, token },
      queryKey
    ),
    queryFn: () =>
      TaskInstanceService.getLog({
        dagId,
        dagRunId,
        fullContent,
        mapIndex,
        taskId,
        taskTryNumber,
        token,
      }) as TData,
    ...options,
  });
export const useDagRunServiceGetDagRuns = <
  TData = Common.DagRunServiceGetDagRunsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
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
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDagRunServiceGetDagRunsKeyFn(
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
      queryKey
    ),
    queryFn: () =>
      DagRunService.getDagRuns({
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
      }) as TData,
    ...options,
  });
export const useDagRunServiceGetDagRun = <
  TData = Common.DagRunServiceGetDagRunDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    dagRunId,
    fields,
  }: {
    dagId: string;
    dagRunId: string;
    fields?: string[];
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDagRunServiceGetDagRunKeyFn(
      { dagId, dagRunId, fields },
      queryKey
    ),
    queryFn: () =>
      DagRunService.getDagRun({ dagId, dagRunId, fields }) as TData,
    ...options,
  });
export const useDagRunServiceGetUpstreamDatasetEvents = <
  TData = Common.DagRunServiceGetUpstreamDatasetEventsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    dagRunId,
  }: {
    dagId: string;
    dagRunId: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDagRunServiceGetUpstreamDatasetEventsKeyFn(
      { dagId, dagRunId },
      queryKey
    ),
    queryFn: () =>
      DagRunService.getUpstreamDatasetEvents({ dagId, dagRunId }) as TData,
    ...options,
  });
export const useDatasetServiceGetUpstreamDatasetEvents = <
  TData = Common.DatasetServiceGetUpstreamDatasetEventsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagId,
    dagRunId,
  }: {
    dagId: string;
    dagRunId: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDatasetServiceGetUpstreamDatasetEventsKeyFn(
      { dagId, dagRunId },
      queryKey
    ),
    queryFn: () =>
      DatasetService.getUpstreamDatasetEvents({ dagId, dagRunId }) as TData,
    ...options,
  });
export const useDatasetServiceGetDagDatasetQueuedEvent = <
  TData = Common.DatasetServiceGetDagDatasetQueuedEventDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    before,
    dagId,
    uri,
  }: {
    before?: string;
    dagId: string;
    uri: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDatasetServiceGetDagDatasetQueuedEventKeyFn(
      { before, dagId, uri },
      queryKey
    ),
    queryFn: () =>
      DatasetService.getDagDatasetQueuedEvent({ before, dagId, uri }) as TData,
    ...options,
  });
export const useDatasetServiceGetDagDatasetQueuedEvents = <
  TData = Common.DatasetServiceGetDagDatasetQueuedEventsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    before,
    dagId,
  }: {
    before?: string;
    dagId: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDatasetServiceGetDagDatasetQueuedEventsKeyFn(
      { before, dagId },
      queryKey
    ),
    queryFn: () =>
      DatasetService.getDagDatasetQueuedEvents({ before, dagId }) as TData,
    ...options,
  });
export const useDatasetServiceGetDatasetQueuedEvents = <
  TData = Common.DatasetServiceGetDatasetQueuedEventsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    before,
    uri,
  }: {
    before?: string;
    uri: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDatasetServiceGetDatasetQueuedEventsKeyFn(
      { before, uri },
      queryKey
    ),
    queryFn: () =>
      DatasetService.getDatasetQueuedEvents({ before, uri }) as TData,
    ...options,
  });
export const useDatasetServiceGetDatasets = <
  TData = Common.DatasetServiceGetDatasetsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
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
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDatasetServiceGetDatasetsKeyFn(
      { dagIds, limit, offset, orderBy, uriPattern },
      queryKey
    ),
    queryFn: () =>
      DatasetService.getDatasets({
        dagIds,
        limit,
        offset,
        orderBy,
        uriPattern,
      }) as TData,
    ...options,
  });
export const useDatasetServiceGetDataset = <
  TData = Common.DatasetServiceGetDatasetDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    uri,
  }: {
    uri: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDatasetServiceGetDatasetKeyFn({ uri }, queryKey),
    queryFn: () => DatasetService.getDataset({ uri }) as TData,
    ...options,
  });
export const useDatasetServiceGetDatasetEvents = <
  TData = Common.DatasetServiceGetDatasetEventsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
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
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDatasetServiceGetDatasetEventsKeyFn(
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
      queryKey
    ),
    queryFn: () =>
      DatasetService.getDatasetEvents({
        datasetId,
        limit,
        offset,
        orderBy,
        sourceDagId,
        sourceMapIndex,
        sourceRunId,
        sourceTaskId,
      }) as TData,
    ...options,
  });
export const useEventLogServiceGetEventLogs = <
  TData = Common.EventLogServiceGetEventLogsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
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
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseEventLogServiceGetEventLogsKeyFn(
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
      queryKey
    ),
    queryFn: () =>
      EventLogService.getEventLogs({
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
      }) as TData,
    ...options,
  });
export const useEventLogServiceGetEventLog = <
  TData = Common.EventLogServiceGetEventLogDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    eventLogId,
  }: {
    eventLogId: number;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseEventLogServiceGetEventLogKeyFn(
      { eventLogId },
      queryKey
    ),
    queryFn: () => EventLogService.getEventLog({ eventLogId }) as TData,
    ...options,
  });
export const useImportErrorServiceGetImportErrors = <
  TData = Common.ImportErrorServiceGetImportErrorsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    limit,
    offset,
    orderBy,
  }: {
    limit?: number;
    offset?: number;
    orderBy?: string;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseImportErrorServiceGetImportErrorsKeyFn(
      { limit, offset, orderBy },
      queryKey
    ),
    queryFn: () =>
      ImportErrorService.getImportErrors({ limit, offset, orderBy }) as TData,
    ...options,
  });
export const useImportErrorServiceGetImportError = <
  TData = Common.ImportErrorServiceGetImportErrorDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    importErrorId,
  }: {
    importErrorId: number;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseImportErrorServiceGetImportErrorKeyFn(
      { importErrorId },
      queryKey
    ),
    queryFn: () =>
      ImportErrorService.getImportError({ importErrorId }) as TData,
    ...options,
  });
export const usePoolServiceGetPools = <
  TData = Common.PoolServiceGetPoolsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    limit,
    offset,
    orderBy,
  }: {
    limit?: number;
    offset?: number;
    orderBy?: string;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UsePoolServiceGetPoolsKeyFn(
      { limit, offset, orderBy },
      queryKey
    ),
    queryFn: () => PoolService.getPools({ limit, offset, orderBy }) as TData,
    ...options,
  });
export const usePoolServiceGetPool = <
  TData = Common.PoolServiceGetPoolDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    poolName,
  }: {
    poolName: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UsePoolServiceGetPoolKeyFn({ poolName }, queryKey),
    queryFn: () => PoolService.getPool({ poolName }) as TData,
    ...options,
  });
export const useProviderServiceGetProviders = <
  TData = Common.ProviderServiceGetProvidersDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseProviderServiceGetProvidersKeyFn(queryKey),
    queryFn: () => ProviderService.getProviders() as TData,
    ...options,
  });
export const useVariableServiceGetVariables = <
  TData = Common.VariableServiceGetVariablesDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    limit,
    offset,
    orderBy,
  }: {
    limit?: number;
    offset?: number;
    orderBy?: string;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseVariableServiceGetVariablesKeyFn(
      { limit, offset, orderBy },
      queryKey
    ),
    queryFn: () =>
      VariableService.getVariables({ limit, offset, orderBy }) as TData,
    ...options,
  });
export const useVariableServiceGetVariable = <
  TData = Common.VariableServiceGetVariableDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    variableKey,
  }: {
    variableKey: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseVariableServiceGetVariableKeyFn(
      { variableKey },
      queryKey
    ),
    queryFn: () => VariableService.getVariable({ variableKey }) as TData,
    ...options,
  });
export const useXcomServiceGetXcomEntries = <
  TData = Common.XcomServiceGetXcomEntriesDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
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
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseXcomServiceGetXcomEntriesKeyFn(
      { dagId, dagRunId, limit, mapIndex, offset, taskId, xcomKey },
      queryKey
    ),
    queryFn: () =>
      XcomService.getXcomEntries({
        dagId,
        dagRunId,
        limit,
        mapIndex,
        offset,
        taskId,
        xcomKey,
      }) as TData,
    ...options,
  });
export const useXcomServiceGetXcomEntry = <
  TData = Common.XcomServiceGetXcomEntryDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
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
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseXcomServiceGetXcomEntryKeyFn(
      { dagId, dagRunId, deserialize, mapIndex, stringify, taskId, xcomKey },
      queryKey
    ),
    queryFn: () =>
      XcomService.getXcomEntry({
        dagId,
        dagRunId,
        deserialize,
        mapIndex,
        stringify,
        taskId,
        xcomKey,
      }) as TData,
    ...options,
  });
export const useDagStatsServiceGetDagStats = <
  TData = Common.DagStatsServiceGetDagStatsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    dagIds,
  }: {
    dagIds: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDagStatsServiceGetDagStatsKeyFn({ dagIds }, queryKey),
    queryFn: () => DagStatsService.getDagStats({ dagIds }) as TData,
    ...options,
  });
export const useDagWarningServiceGetDagWarnings = <
  TData = Common.DagWarningServiceGetDagWarningsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
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
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseDagWarningServiceGetDagWarningsKeyFn(
      { dagId, limit, offset, orderBy, warningType },
      queryKey
    ),
    queryFn: () =>
      DagWarningService.getDagWarnings({
        dagId,
        limit,
        offset,
        orderBy,
        warningType,
      }) as TData,
    ...options,
  });
export const useConfigServiceGetConfig = <
  TData = Common.ConfigServiceGetConfigDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    section,
  }: {
    section?: string;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseConfigServiceGetConfigKeyFn({ section }, queryKey),
    queryFn: () => ConfigService.getConfig({ section }) as TData,
    ...options,
  });
export const useConfigServiceGetValue = <
  TData = Common.ConfigServiceGetValueDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    option,
    section,
  }: {
    option: string;
    section: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseConfigServiceGetValueKeyFn(
      { option, section },
      queryKey
    ),
    queryFn: () => ConfigService.getValue({ option, section }) as TData,
    ...options,
  });
export const useMonitoringServiceGetHealth = <
  TData = Common.MonitoringServiceGetHealthDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseMonitoringServiceGetHealthKeyFn(queryKey),
    queryFn: () => MonitoringService.getHealth() as TData,
    ...options,
  });
export const useMonitoringServiceGetVersion = <
  TData = Common.MonitoringServiceGetVersionDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseMonitoringServiceGetVersionKeyFn(queryKey),
    queryFn: () => MonitoringService.getVersion() as TData,
    ...options,
  });
export const usePluginServiceGetPlugins = <
  TData = Common.PluginServiceGetPluginsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    limit,
    offset,
  }: {
    limit?: number;
    offset?: number;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UsePluginServiceGetPluginsKeyFn(
      { limit, offset },
      queryKey
    ),
    queryFn: () => PluginService.getPlugins({ limit, offset }) as TData,
    ...options,
  });
export const useRoleServiceGetRoles = <
  TData = Common.RoleServiceGetRolesDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    limit,
    offset,
    orderBy,
  }: {
    limit?: number;
    offset?: number;
    orderBy?: string;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseRoleServiceGetRolesKeyFn(
      { limit, offset, orderBy },
      queryKey
    ),
    queryFn: () => RoleService.getRoles({ limit, offset, orderBy }) as TData,
    ...options,
  });
export const useRoleServiceGetRole = <
  TData = Common.RoleServiceGetRoleDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    roleName,
  }: {
    roleName: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseRoleServiceGetRoleKeyFn({ roleName }, queryKey),
    queryFn: () => RoleService.getRole({ roleName }) as TData,
    ...options,
  });
export const usePermissionServiceGetPermissions = <
  TData = Common.PermissionServiceGetPermissionsDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    limit,
    offset,
  }: {
    limit?: number;
    offset?: number;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UsePermissionServiceGetPermissionsKeyFn(
      { limit, offset },
      queryKey
    ),
    queryFn: () => PermissionService.getPermissions({ limit, offset }) as TData,
    ...options,
  });
export const useUserServiceGetUsers = <
  TData = Common.UserServiceGetUsersDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    limit,
    offset,
    orderBy,
  }: {
    limit?: number;
    offset?: number;
    orderBy?: string;
  } = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseUserServiceGetUsersKeyFn(
      { limit, offset, orderBy },
      queryKey
    ),
    queryFn: () => UserService.getUsers({ limit, offset, orderBy }) as TData,
    ...options,
  });
export const useUserServiceGetUser = <
  TData = Common.UserServiceGetUserDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  {
    username,
  }: {
    username: string;
  },
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseUserServiceGetUserKeyFn({ username }, queryKey),
    queryFn: () => UserService.getUser({ username }) as TData,
    ...options,
  });
export const useConnectionServicePostConnection = <
  TData = Common.ConnectionServicePostConnectionMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        requestBody: Connection;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      requestBody: Connection;
    },
    TContext
  >({
    mutationFn: ({ requestBody }) =>
      ConnectionService.postConnection({
        requestBody,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const useConnectionServiceTestConnection = <
  TData = Common.ConnectionServiceTestConnectionMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        requestBody: Connection;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      requestBody: Connection;
    },
    TContext
  >({
    mutationFn: ({ requestBody }) =>
      ConnectionService.testConnection({
        requestBody,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const useDagServicePostClearTaskInstances = <
  TData = Common.DagServicePostClearTaskInstancesMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        dagId: string;
        requestBody: ClearTaskInstances;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      dagId: string;
      requestBody: ClearTaskInstances;
    },
    TContext
  >({
    mutationFn: ({ dagId, requestBody }) =>
      DagService.postClearTaskInstances({
        dagId,
        requestBody,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const useDagServicePostSetTaskInstancesState = <
  TData = Common.DagServicePostSetTaskInstancesStateMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        dagId: string;
        requestBody: UpdateTaskInstancesState;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      dagId: string;
      requestBody: UpdateTaskInstancesState;
    },
    TContext
  >({
    mutationFn: ({ dagId, requestBody }) =>
      DagService.postSetTaskInstancesState({
        dagId,
        requestBody,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const useTaskInstanceServiceGetTaskInstancesBatch = <
  TData = Common.TaskInstanceServiceGetTaskInstancesBatchMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        requestBody: ListTaskInstanceForm;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      requestBody: ListTaskInstanceForm;
    },
    TContext
  >({
    mutationFn: ({ requestBody }) =>
      TaskInstanceService.getTaskInstancesBatch({
        requestBody,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const useDagRunServicePostDagRun = <
  TData = Common.DagRunServicePostDagRunMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        dagId: string;
        requestBody: DAGRun;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      dagId: string;
      requestBody: DAGRun;
    },
    TContext
  >({
    mutationFn: ({ dagId, requestBody }) =>
      DagRunService.postDagRun({
        dagId,
        requestBody,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const useDagRunServiceGetDagRunsBatch = <
  TData = Common.DagRunServiceGetDagRunsBatchMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        requestBody: ListDagRunsForm;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      requestBody: ListDagRunsForm;
    },
    TContext
  >({
    mutationFn: ({ requestBody }) =>
      DagRunService.getDagRunsBatch({
        requestBody,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const useDagRunServiceClearDagRun = <
  TData = Common.DagRunServiceClearDagRunMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        dagId: string;
        dagRunId: string;
        requestBody: ClearDagRun;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      dagId: string;
      dagRunId: string;
      requestBody: ClearDagRun;
    },
    TContext
  >({
    mutationFn: ({ dagId, dagRunId, requestBody }) =>
      DagRunService.clearDagRun({
        dagId,
        dagRunId,
        requestBody,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const useDatasetServiceCreateDatasetEvent = <
  TData = Common.DatasetServiceCreateDatasetEventMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        requestBody: CreateDatasetEvent;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      requestBody: CreateDatasetEvent;
    },
    TContext
  >({
    mutationFn: ({ requestBody }) =>
      DatasetService.createDatasetEvent({
        requestBody,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const usePoolServicePostPool = <
  TData = Common.PoolServicePostPoolMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        requestBody: Pool;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      requestBody: Pool;
    },
    TContext
  >({
    mutationFn: ({ requestBody }) =>
      PoolService.postPool({ requestBody }) as unknown as Promise<TData>,
    ...options,
  });
export const useVariableServicePostVariables = <
  TData = Common.VariableServicePostVariablesMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        requestBody: Variable;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      requestBody: Variable;
    },
    TContext
  >({
    mutationFn: ({ requestBody }) =>
      VariableService.postVariables({
        requestBody,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const useRoleServicePostRole = <
  TData = Common.RoleServicePostRoleMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        requestBody: Role;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      requestBody: Role;
    },
    TContext
  >({
    mutationFn: ({ requestBody }) =>
      RoleService.postRole({ requestBody }) as unknown as Promise<TData>,
    ...options,
  });
export const useUserServicePostUser = <
  TData = Common.UserServicePostUserMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        requestBody: User;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      requestBody: User;
    },
    TContext
  >({
    mutationFn: ({ requestBody }) =>
      UserService.postUser({ requestBody }) as unknown as Promise<TData>,
    ...options,
  });
export const useDagServiceReparseDagFile = <
  TData = Common.DagServiceReparseDagFileMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        fileToken: string;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      fileToken: string;
    },
    TContext
  >({
    mutationFn: ({ fileToken }) =>
      DagService.reparseDagFile({ fileToken }) as unknown as Promise<TData>,
    ...options,
  });
export const useConnectionServicePatchConnection = <
  TData = Common.ConnectionServicePatchConnectionMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        connectionId: string;
        requestBody: Connection;
        updateMask?: string[];
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      connectionId: string;
      requestBody: Connection;
      updateMask?: string[];
    },
    TContext
  >({
    mutationFn: ({ connectionId, requestBody, updateMask }) =>
      ConnectionService.patchConnection({
        connectionId,
        requestBody,
        updateMask,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const useDagServicePatchDags = <
  TData = Common.DagServicePatchDagsMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        dagIdPattern: string;
        limit?: number;
        offset?: number;
        onlyActive?: boolean;
        requestBody: DAG;
        tags?: string[];
        updateMask?: string[];
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      dagIdPattern: string;
      limit?: number;
      offset?: number;
      onlyActive?: boolean;
      requestBody: DAG;
      tags?: string[];
      updateMask?: string[];
    },
    TContext
  >({
    mutationFn: ({
      dagIdPattern,
      limit,
      offset,
      onlyActive,
      requestBody,
      tags,
      updateMask,
    }) =>
      DagService.patchDags({
        dagIdPattern,
        limit,
        offset,
        onlyActive,
        requestBody,
        tags,
        updateMask,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const useDagServicePatchDag = <
  TData = Common.DagServicePatchDagMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        dagId: string;
        requestBody: DAG;
        updateMask?: string[];
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      dagId: string;
      requestBody: DAG;
      updateMask?: string[];
    },
    TContext
  >({
    mutationFn: ({ dagId, requestBody, updateMask }) =>
      DagService.patchDag({
        dagId,
        requestBody,
        updateMask,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const useTaskInstanceServiceSetTaskInstanceNote = <
  TData = Common.TaskInstanceServiceSetTaskInstanceNoteMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        dagId: string;
        dagRunId: string;
        requestBody: SetTaskInstanceNote;
        taskId: string;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      dagId: string;
      dagRunId: string;
      requestBody: SetTaskInstanceNote;
      taskId: string;
    },
    TContext
  >({
    mutationFn: ({ dagId, dagRunId, requestBody, taskId }) =>
      TaskInstanceService.setTaskInstanceNote({
        dagId,
        dagRunId,
        requestBody,
        taskId,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const useTaskInstanceServiceSetMappedTaskInstanceNote = <
  TData = Common.TaskInstanceServiceSetMappedTaskInstanceNoteMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        dagId: string;
        dagRunId: string;
        mapIndex: number;
        requestBody: SetTaskInstanceNote;
        taskId: string;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      dagId: string;
      dagRunId: string;
      mapIndex: number;
      requestBody: SetTaskInstanceNote;
      taskId: string;
    },
    TContext
  >({
    mutationFn: ({ dagId, dagRunId, mapIndex, requestBody, taskId }) =>
      TaskInstanceService.setMappedTaskInstanceNote({
        dagId,
        dagRunId,
        mapIndex,
        requestBody,
        taskId,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const useTaskInstanceServicePatchTaskInstance = <
  TData = Common.TaskInstanceServicePatchTaskInstanceMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        dagId: string;
        dagRunId: string;
        requestBody: UpdateTaskInstance;
        taskId: string;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      dagId: string;
      dagRunId: string;
      requestBody: UpdateTaskInstance;
      taskId: string;
    },
    TContext
  >({
    mutationFn: ({ dagId, dagRunId, requestBody, taskId }) =>
      TaskInstanceService.patchTaskInstance({
        dagId,
        dagRunId,
        requestBody,
        taskId,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const useTaskInstanceServicePatchMappedTaskInstance = <
  TData = Common.TaskInstanceServicePatchMappedTaskInstanceMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        dagId: string;
        dagRunId: string;
        mapIndex: number;
        requestBody?: UpdateTaskInstance;
        taskId: string;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      dagId: string;
      dagRunId: string;
      mapIndex: number;
      requestBody?: UpdateTaskInstance;
      taskId: string;
    },
    TContext
  >({
    mutationFn: ({ dagId, dagRunId, mapIndex, requestBody, taskId }) =>
      TaskInstanceService.patchMappedTaskInstance({
        dagId,
        dagRunId,
        mapIndex,
        requestBody,
        taskId,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const useDagRunServiceUpdateDagRunState = <
  TData = Common.DagRunServiceUpdateDagRunStateMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        dagId: string;
        dagRunId: string;
        requestBody: UpdateDagRunState;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      dagId: string;
      dagRunId: string;
      requestBody: UpdateDagRunState;
    },
    TContext
  >({
    mutationFn: ({ dagId, dagRunId, requestBody }) =>
      DagRunService.updateDagRunState({
        dagId,
        dagRunId,
        requestBody,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const useDagRunServiceSetDagRunNote = <
  TData = Common.DagRunServiceSetDagRunNoteMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        dagId: string;
        dagRunId: string;
        requestBody: SetDagRunNote;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      dagId: string;
      dagRunId: string;
      requestBody: SetDagRunNote;
    },
    TContext
  >({
    mutationFn: ({ dagId, dagRunId, requestBody }) =>
      DagRunService.setDagRunNote({
        dagId,
        dagRunId,
        requestBody,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const usePoolServicePatchPool = <
  TData = Common.PoolServicePatchPoolMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        poolName: string;
        requestBody: Pool;
        updateMask?: string[];
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      poolName: string;
      requestBody: Pool;
      updateMask?: string[];
    },
    TContext
  >({
    mutationFn: ({ poolName, requestBody, updateMask }) =>
      PoolService.patchPool({
        poolName,
        requestBody,
        updateMask,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const useVariableServicePatchVariable = <
  TData = Common.VariableServicePatchVariableMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        requestBody: Variable;
        updateMask?: string[];
        variableKey: string;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      requestBody: Variable;
      updateMask?: string[];
      variableKey: string;
    },
    TContext
  >({
    mutationFn: ({ requestBody, updateMask, variableKey }) =>
      VariableService.patchVariable({
        requestBody,
        updateMask,
        variableKey,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const useRoleServicePatchRole = <
  TData = Common.RoleServicePatchRoleMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        requestBody: Role;
        roleName: string;
        updateMask?: string[];
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      requestBody: Role;
      roleName: string;
      updateMask?: string[];
    },
    TContext
  >({
    mutationFn: ({ requestBody, roleName, updateMask }) =>
      RoleService.patchRole({
        requestBody,
        roleName,
        updateMask,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const useUserServicePatchUser = <
  TData = Common.UserServicePatchUserMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        requestBody: User;
        updateMask?: string[];
        username: string;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      requestBody: User;
      updateMask?: string[];
      username: string;
    },
    TContext
  >({
    mutationFn: ({ requestBody, updateMask, username }) =>
      UserService.patchUser({
        requestBody,
        updateMask,
        username,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const useConnectionServiceDeleteConnection = <
  TData = Common.ConnectionServiceDeleteConnectionMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        connectionId: string;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      connectionId: string;
    },
    TContext
  >({
    mutationFn: ({ connectionId }) =>
      ConnectionService.deleteConnection({
        connectionId,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const useDagServiceDeleteDag = <
  TData = Common.DagServiceDeleteDagMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        dagId: string;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      dagId: string;
    },
    TContext
  >({
    mutationFn: ({ dagId }) =>
      DagService.deleteDag({ dagId }) as unknown as Promise<TData>,
    ...options,
  });
export const useDagRunServiceDeleteDagRun = <
  TData = Common.DagRunServiceDeleteDagRunMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        dagId: string;
        dagRunId: string;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      dagId: string;
      dagRunId: string;
    },
    TContext
  >({
    mutationFn: ({ dagId, dagRunId }) =>
      DagRunService.deleteDagRun({
        dagId,
        dagRunId,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const useDatasetServiceDeleteDagDatasetQueuedEvent = <
  TData = Common.DatasetServiceDeleteDagDatasetQueuedEventMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        before?: string;
        dagId: string;
        uri: string;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      before?: string;
      dagId: string;
      uri: string;
    },
    TContext
  >({
    mutationFn: ({ before, dagId, uri }) =>
      DatasetService.deleteDagDatasetQueuedEvent({
        before,
        dagId,
        uri,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const useDatasetServiceDeleteDagDatasetQueuedEvents = <
  TData = Common.DatasetServiceDeleteDagDatasetQueuedEventsMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        before?: string;
        dagId: string;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      before?: string;
      dagId: string;
    },
    TContext
  >({
    mutationFn: ({ before, dagId }) =>
      DatasetService.deleteDagDatasetQueuedEvents({
        before,
        dagId,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const useDatasetServiceDeleteDatasetQueuedEvents = <
  TData = Common.DatasetServiceDeleteDatasetQueuedEventsMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        before?: string;
        uri: string;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      before?: string;
      uri: string;
    },
    TContext
  >({
    mutationFn: ({ before, uri }) =>
      DatasetService.deleteDatasetQueuedEvents({
        before,
        uri,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const usePoolServiceDeletePool = <
  TData = Common.PoolServiceDeletePoolMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        poolName: string;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      poolName: string;
    },
    TContext
  >({
    mutationFn: ({ poolName }) =>
      PoolService.deletePool({ poolName }) as unknown as Promise<TData>,
    ...options,
  });
export const useVariableServiceDeleteVariable = <
  TData = Common.VariableServiceDeleteVariableMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        variableKey: string;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      variableKey: string;
    },
    TContext
  >({
    mutationFn: ({ variableKey }) =>
      VariableService.deleteVariable({
        variableKey,
      }) as unknown as Promise<TData>,
    ...options,
  });
export const useRoleServiceDeleteRole = <
  TData = Common.RoleServiceDeleteRoleMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        roleName: string;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      roleName: string;
    },
    TContext
  >({
    mutationFn: ({ roleName }) =>
      RoleService.deleteRole({ roleName }) as unknown as Promise<TData>,
    ...options,
  });
export const useUserServiceDeleteUser = <
  TData = Common.UserServiceDeleteUserMutationResult,
  TError = unknown,
  TContext = unknown,
>(
  options?: Omit<
    UseMutationOptions<
      TData,
      TError,
      {
        username: string;
      },
      TContext
    >,
    "mutationFn"
  >
) =>
  useMutation<
    TData,
    TError,
    {
      username: string;
    },
    TContext
  >({
    mutationFn: ({ username }) =>
      UserService.deleteUser({ username }) as unknown as Promise<TData>,
    ...options,
  });
