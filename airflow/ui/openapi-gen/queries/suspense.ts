// generated with @7nohe/openapi-react-query-codegen@1.6.0

import { UseQueryOptions, useSuspenseQuery } from "@tanstack/react-query";
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
import * as Common from "./common";
export const useConnectionServiceGetConnectionsSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseConnectionServiceGetConnectionsKeyFn(
      { limit, offset, orderBy },
      queryKey
    ),
    queryFn: () =>
      ConnectionService.getConnections({ limit, offset, orderBy }) as TData,
    ...options,
  });
export const useConnectionServiceGetConnectionSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseConnectionServiceGetConnectionKeyFn(
      { connectionId },
      queryKey
    ),
    queryFn: () => ConnectionService.getConnection({ connectionId }) as TData,
    ...options,
  });
export const useDagServiceGetDagsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useDagServiceGetDagSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseDagServiceGetDagKeyFn({ dagId, fields }, queryKey),
    queryFn: () => DagService.getDag({ dagId, fields }) as TData,
    ...options,
  });
export const useDagServiceGetDagDetailsSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseDagServiceGetDagDetailsKeyFn(
      { dagId, fields },
      queryKey
    ),
    queryFn: () => DagService.getDagDetails({ dagId, fields }) as TData,
    ...options,
  });
export const useDagServiceGetTasksSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseDagServiceGetTasksKeyFn({ dagId, orderBy }, queryKey),
    queryFn: () => DagService.getTasks({ dagId, orderBy }) as TData,
    ...options,
  });
export const useDagServiceGetTaskSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseDagServiceGetTaskKeyFn({ dagId, taskId }, queryKey),
    queryFn: () => DagService.getTask({ dagId, taskId }) as TData,
    ...options,
  });
export const useDagServiceGetDagSourceSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseDagServiceGetDagSourceKeyFn({ fileToken }, queryKey),
    queryFn: () => DagService.getDagSource({ fileToken }) as TData,
    ...options,
  });
export const useTaskInstanceServiceGetTaskInstanceDependenciesSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useTaskInstanceServiceGetMappedTaskInstanceDependenciesSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useTaskInstanceServiceGetTaskInstancesSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useTaskInstanceServiceGetTaskInstanceSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseTaskInstanceServiceGetTaskInstanceKeyFn(
      { dagId, dagRunId, taskId },
      queryKey
    ),
    queryFn: () =>
      TaskInstanceService.getTaskInstance({ dagId, dagRunId, taskId }) as TData,
    ...options,
  });
export const useTaskInstanceServiceGetMappedTaskInstanceSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useTaskInstanceServiceGetMappedTaskInstancesSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useTaskInstanceServiceGetTaskInstanceTryDetailsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useTaskInstanceServiceGetTaskInstanceTriesSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useTaskInstanceServiceGetMappedTaskInstanceTriesSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useTaskInstanceServiceGetMappedTaskInstanceTryDetailsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useTaskInstanceServiceGetExtraLinksSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseTaskInstanceServiceGetExtraLinksKeyFn(
      { dagId, dagRunId, taskId },
      queryKey
    ),
    queryFn: () =>
      TaskInstanceService.getExtraLinks({ dagId, dagRunId, taskId }) as TData,
    ...options,
  });
export const useTaskInstanceServiceGetLogSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useDagRunServiceGetDagRunsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useDagRunServiceGetDagRunSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseDagRunServiceGetDagRunKeyFn(
      { dagId, dagRunId, fields },
      queryKey
    ),
    queryFn: () =>
      DagRunService.getDagRun({ dagId, dagRunId, fields }) as TData,
    ...options,
  });
export const useDagRunServiceGetUpstreamDatasetEventsSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseDagRunServiceGetUpstreamDatasetEventsKeyFn(
      { dagId, dagRunId },
      queryKey
    ),
    queryFn: () =>
      DagRunService.getUpstreamDatasetEvents({ dagId, dagRunId }) as TData,
    ...options,
  });
export const useDatasetServiceGetUpstreamDatasetEventsSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseDatasetServiceGetUpstreamDatasetEventsKeyFn(
      { dagId, dagRunId },
      queryKey
    ),
    queryFn: () =>
      DatasetService.getUpstreamDatasetEvents({ dagId, dagRunId }) as TData,
    ...options,
  });
export const useDatasetServiceGetDagDatasetQueuedEventSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseDatasetServiceGetDagDatasetQueuedEventKeyFn(
      { before, dagId, uri },
      queryKey
    ),
    queryFn: () =>
      DatasetService.getDagDatasetQueuedEvent({ before, dagId, uri }) as TData,
    ...options,
  });
export const useDatasetServiceGetDagDatasetQueuedEventsSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseDatasetServiceGetDagDatasetQueuedEventsKeyFn(
      { before, dagId },
      queryKey
    ),
    queryFn: () =>
      DatasetService.getDagDatasetQueuedEvents({ before, dagId }) as TData,
    ...options,
  });
export const useDatasetServiceGetDatasetQueuedEventsSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseDatasetServiceGetDatasetQueuedEventsKeyFn(
      { before, uri },
      queryKey
    ),
    queryFn: () =>
      DatasetService.getDatasetQueuedEvents({ before, uri }) as TData,
    ...options,
  });
export const useDatasetServiceGetDatasetsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useDatasetServiceGetDatasetSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseDatasetServiceGetDatasetKeyFn({ uri }, queryKey),
    queryFn: () => DatasetService.getDataset({ uri }) as TData,
    ...options,
  });
export const useDatasetServiceGetDatasetEventsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useEventLogServiceGetEventLogsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useEventLogServiceGetEventLogSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseEventLogServiceGetEventLogKeyFn(
      { eventLogId },
      queryKey
    ),
    queryFn: () => EventLogService.getEventLog({ eventLogId }) as TData,
    ...options,
  });
export const useImportErrorServiceGetImportErrorsSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseImportErrorServiceGetImportErrorsKeyFn(
      { limit, offset, orderBy },
      queryKey
    ),
    queryFn: () =>
      ImportErrorService.getImportErrors({ limit, offset, orderBy }) as TData,
    ...options,
  });
export const useImportErrorServiceGetImportErrorSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseImportErrorServiceGetImportErrorKeyFn(
      { importErrorId },
      queryKey
    ),
    queryFn: () =>
      ImportErrorService.getImportError({ importErrorId }) as TData,
    ...options,
  });
export const usePoolServiceGetPoolsSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UsePoolServiceGetPoolsKeyFn(
      { limit, offset, orderBy },
      queryKey
    ),
    queryFn: () => PoolService.getPools({ limit, offset, orderBy }) as TData,
    ...options,
  });
export const usePoolServiceGetPoolSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UsePoolServiceGetPoolKeyFn({ poolName }, queryKey),
    queryFn: () => PoolService.getPool({ poolName }) as TData,
    ...options,
  });
export const useProviderServiceGetProvidersSuspense = <
  TData = Common.ProviderServiceGetProvidersDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseProviderServiceGetProvidersKeyFn(queryKey),
    queryFn: () => ProviderService.getProviders() as TData,
    ...options,
  });
export const useVariableServiceGetVariablesSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseVariableServiceGetVariablesKeyFn(
      { limit, offset, orderBy },
      queryKey
    ),
    queryFn: () =>
      VariableService.getVariables({ limit, offset, orderBy }) as TData,
    ...options,
  });
export const useVariableServiceGetVariableSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseVariableServiceGetVariableKeyFn(
      { variableKey },
      queryKey
    ),
    queryFn: () => VariableService.getVariable({ variableKey }) as TData,
    ...options,
  });
export const useXcomServiceGetXcomEntriesSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useXcomServiceGetXcomEntrySuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useDagStatsServiceGetDagStatsSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseDagStatsServiceGetDagStatsKeyFn({ dagIds }, queryKey),
    queryFn: () => DagStatsService.getDagStats({ dagIds }) as TData,
    ...options,
  });
export const useDagWarningServiceGetDagWarningsSuspense = <
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
  useSuspenseQuery<TData, TError>({
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
export const useConfigServiceGetConfigSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseConfigServiceGetConfigKeyFn({ section }, queryKey),
    queryFn: () => ConfigService.getConfig({ section }) as TData,
    ...options,
  });
export const useConfigServiceGetValueSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseConfigServiceGetValueKeyFn(
      { option, section },
      queryKey
    ),
    queryFn: () => ConfigService.getValue({ option, section }) as TData,
    ...options,
  });
export const useMonitoringServiceGetHealthSuspense = <
  TData = Common.MonitoringServiceGetHealthDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseMonitoringServiceGetHealthKeyFn(queryKey),
    queryFn: () => MonitoringService.getHealth() as TData,
    ...options,
  });
export const useMonitoringServiceGetVersionSuspense = <
  TData = Common.MonitoringServiceGetVersionDefaultResponse,
  TError = unknown,
  TQueryKey extends Array<unknown> = unknown[],
>(
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">
) =>
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseMonitoringServiceGetVersionKeyFn(queryKey),
    queryFn: () => MonitoringService.getVersion() as TData,
    ...options,
  });
export const usePluginServiceGetPluginsSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UsePluginServiceGetPluginsKeyFn(
      { limit, offset },
      queryKey
    ),
    queryFn: () => PluginService.getPlugins({ limit, offset }) as TData,
    ...options,
  });
export const useRoleServiceGetRolesSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseRoleServiceGetRolesKeyFn(
      { limit, offset, orderBy },
      queryKey
    ),
    queryFn: () => RoleService.getRoles({ limit, offset, orderBy }) as TData,
    ...options,
  });
export const useRoleServiceGetRoleSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseRoleServiceGetRoleKeyFn({ roleName }, queryKey),
    queryFn: () => RoleService.getRole({ roleName }) as TData,
    ...options,
  });
export const usePermissionServiceGetPermissionsSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UsePermissionServiceGetPermissionsKeyFn(
      { limit, offset },
      queryKey
    ),
    queryFn: () => PermissionService.getPermissions({ limit, offset }) as TData,
    ...options,
  });
export const useUserServiceGetUsersSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseUserServiceGetUsersKeyFn(
      { limit, offset, orderBy },
      queryKey
    ),
    queryFn: () => UserService.getUsers({ limit, offset, orderBy }) as TData,
    ...options,
  });
export const useUserServiceGetUserSuspense = <
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
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseUserServiceGetUserKeyFn({ username }, queryKey),
    queryFn: () => UserService.getUser({ username }) as TData,
    ...options,
  });
