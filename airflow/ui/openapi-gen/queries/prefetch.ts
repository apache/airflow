// generated with @7nohe/openapi-react-query-codegen@1.6.0
import { type QueryClient } from "@tanstack/react-query";

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

/**
 * List connections
 * @param data The data for the request.
 * @param data.limit The numbers of items to return.
 * @param data.offset The number of items to skip before starting to collect the result set.
 * @param data.orderBy The name of the field to order the results by.
 * Prefix a field name with `-` to reverse the sort order.
 *
 * *New in version 2.1.0*
 *
 * @returns ConnectionCollection Success.
 * @throws ApiError
 */
export const prefetchUseConnectionServiceGetConnections = (
  queryClient: QueryClient,
  {
    limit,
    offset,
    orderBy,
  }: {
    limit?: number;
    offset?: number;
    orderBy?: string;
  } = {},
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseConnectionServiceGetConnectionsKeyFn({
      limit,
      offset,
      orderBy,
    }),
    queryFn: () => ConnectionService.getConnections({ limit, offset, orderBy }),
  });
/**
 * Get a connection
 * @param data The data for the request.
 * @param data.connectionId The connection ID.
 * @returns Connection Success.
 * @throws ApiError
 */
export const prefetchUseConnectionServiceGetConnection = (
  queryClient: QueryClient,
  {
    connectionId,
  }: {
    connectionId: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseConnectionServiceGetConnectionKeyFn({ connectionId }),
    queryFn: () => ConnectionService.getConnection({ connectionId }),
  });
/**
 * List DAGs
 * List DAGs in the database.
 * `dag_id_pattern` can be set to match dags of a specific pattern
 *
 * @param data The data for the request.
 * @param data.limit The numbers of items to return.
 * @param data.offset The number of items to skip before starting to collect the result set.
 * @param data.orderBy The name of the field to order the results by.
 * Prefix a field name with `-` to reverse the sort order.
 *
 * *New in version 2.1.0*
 *
 * @param data.tags List of tags to filter results.
 *
 * *New in version 2.2.0*
 *
 * @param data.onlyActive Only filter active DAGs.
 *
 * *New in version 2.1.1*
 *
 * @param data.paused Only filter paused/unpaused DAGs. If absent or null, it returns paused and unpaused DAGs.
 *
 * *New in version 2.6.0*
 *
 * @param data.fields List of field for return.
 *
 * @param data.dagIdPattern If set, only return DAGs with dag_ids matching this pattern.
 *
 * @returns DAGCollection Success.
 * @throws ApiError
 */
export const prefetchUseDagServiceGetDags = (
  queryClient: QueryClient,
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
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagServiceGetDagsKeyFn({
      dagIdPattern,
      fields,
      limit,
      offset,
      onlyActive,
      orderBy,
      paused,
      tags,
    }),
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
      }),
  });
/**
 * Get basic information about a DAG
 * Presents only information available in database (DAGModel).
 * If you need detailed information, consider using GET /dags/{dag_id}/details.
 *
 * @param data The data for the request.
 * @param data.dagId The DAG ID.
 * @param data.fields List of field for return.
 *
 * @returns DAG Success.
 * @throws ApiError
 */
export const prefetchUseDagServiceGetDag = (
  queryClient: QueryClient,
  {
    dagId,
    fields,
  }: {
    dagId: string;
    fields?: string[];
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagServiceGetDagKeyFn({ dagId, fields }),
    queryFn: () => DagService.getDag({ dagId, fields }),
  });
/**
 * Get a simplified representation of DAG
 * The response contains many DAG attributes, so the response can be large. If possible, consider using GET /dags/{dag_id}.
 *
 * @param data The data for the request.
 * @param data.dagId The DAG ID.
 * @param data.fields List of field for return.
 *
 * @returns DAGDetail Success.
 * @throws ApiError
 */
export const prefetchUseDagServiceGetDagDetails = (
  queryClient: QueryClient,
  {
    dagId,
    fields,
  }: {
    dagId: string;
    fields?: string[];
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagServiceGetDagDetailsKeyFn({ dagId, fields }),
    queryFn: () => DagService.getDagDetails({ dagId, fields }),
  });
/**
 * Get tasks for DAG
 * @param data The data for the request.
 * @param data.dagId The DAG ID.
 * @param data.orderBy The name of the field to order the results by.
 * Prefix a field name with `-` to reverse the sort order.
 *
 * *New in version 2.1.0*
 *
 * @returns TaskCollection Success.
 * @throws ApiError
 */
export const prefetchUseDagServiceGetTasks = (
  queryClient: QueryClient,
  {
    dagId,
    orderBy,
  }: {
    dagId: string;
    orderBy?: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagServiceGetTasksKeyFn({ dagId, orderBy }),
    queryFn: () => DagService.getTasks({ dagId, orderBy }),
  });
/**
 * Get simplified representation of a task
 * @param data The data for the request.
 * @param data.dagId The DAG ID.
 * @param data.taskId The task ID.
 * @returns Task Success.
 * @throws ApiError
 */
export const prefetchUseDagServiceGetTask = (
  queryClient: QueryClient,
  {
    dagId,
    taskId,
  }: {
    dagId: string;
    taskId: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagServiceGetTaskKeyFn({ dagId, taskId }),
    queryFn: () => DagService.getTask({ dagId, taskId }),
  });
/**
 * Get a source code
 * Get a source code using file token.
 *
 * @param data The data for the request.
 * @param data.fileToken The key containing the encrypted path to the file. Encryption and decryption take place only on
 * the server. This prevents the client from reading an non-DAG file. This also ensures API
 * extensibility, because the format of encrypted data may change.
 *
 * @returns unknown Success.
 * @throws ApiError
 */
export const prefetchUseDagServiceGetDagSource = (
  queryClient: QueryClient,
  {
    fileToken,
  }: {
    fileToken: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagServiceGetDagSourceKeyFn({ fileToken }),
    queryFn: () => DagService.getDagSource({ fileToken }),
  });
/**
 * Get task dependencies blocking task from getting scheduled.
 * Get task dependencies blocking task from getting scheduled.
 *
 * *New in version 2.10.0*
 *
 * @param data The data for the request.
 * @param data.dagId The DAG ID.
 * @param data.dagRunId The DAG run ID.
 * @param data.taskId The task ID.
 * @returns TaskInstanceDependencyCollection Success.
 * @throws ApiError
 */
export const prefetchUseTaskInstanceServiceGetTaskInstanceDependencies = (
  queryClient: QueryClient,
  {
    dagId,
    dagRunId,
    taskId,
  }: {
    dagId: string;
    dagRunId: string;
    taskId: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseTaskInstanceServiceGetTaskInstanceDependenciesKeyFn({
      dagId,
      dagRunId,
      taskId,
    }),
    queryFn: () =>
      TaskInstanceService.getTaskInstanceDependencies({
        dagId,
        dagRunId,
        taskId,
      }),
  });
/**
 * Get task dependencies blocking task from getting scheduled.
 * Get task dependencies blocking task from getting scheduled.
 *
 * *New in version 2.10.0*
 *
 * @param data The data for the request.
 * @param data.dagId The DAG ID.
 * @param data.dagRunId The DAG run ID.
 * @param data.taskId The task ID.
 * @param data.mapIndex The map index.
 * @returns TaskInstanceDependencyCollection Success.
 * @throws ApiError
 */
export const prefetchUseTaskInstanceServiceGetMappedTaskInstanceDependencies = (
  queryClient: QueryClient,
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
) =>
  queryClient.prefetchQuery({
    queryKey:
      Common.UseTaskInstanceServiceGetMappedTaskInstanceDependenciesKeyFn({
        dagId,
        dagRunId,
        mapIndex,
        taskId,
      }),
    queryFn: () =>
      TaskInstanceService.getMappedTaskInstanceDependencies({
        dagId,
        dagRunId,
        mapIndex,
        taskId,
      }),
  });
/**
 * List task instances
 * This endpoint allows specifying `~` as the dag_id, dag_run_id to retrieve DAG runs for all DAGs and DAG runs.
 *
 * @param data The data for the request.
 * @param data.dagId The DAG ID.
 * @param data.dagRunId The DAG run ID.
 * @param data.limit The numbers of items to return.
 * @param data.offset The number of items to skip before starting to collect the result set.
 * @param data.orderBy The name of the field to order the results by. Prefix a field name
 * with `-` to reverse the sort order. `order_by` defaults to
 * `map_index` when unspecified.
 * Supported field names: `state`, `duration`, `start_date`, `end_date`
 * and `map_index`.
 *
 * *New in version 3.0.0*
 *
 * @param data.executionDateGte Returns objects greater or equal to the specified date.
 *
 * This can be combined with execution_date_lte parameter to receive only the selected period.
 *
 * @param data.executionDateLte Returns objects less than or equal to the specified date.
 *
 * This can be combined with execution_date_gte parameter to receive only the selected period.
 *
 * @param data.startDateGte Returns objects greater or equal the specified date.
 *
 * This can be combined with start_date_lte parameter to receive only the selected period.
 *
 * @param data.startDateLte Returns objects less or equal the specified date.
 *
 * This can be combined with start_date_gte parameter to receive only the selected period.
 *
 * @param data.endDateGte Returns objects greater or equal the specified date.
 *
 * This can be combined with start_date_lte parameter to receive only the selected period.
 *
 * @param data.endDateLte Returns objects less than or equal to the specified date.
 *
 * This can be combined with start_date_gte parameter to receive only the selected period.
 *
 * @param data.updatedAtGte Returns objects greater or equal the specified date.
 *
 * This can be combined with updated_at_lte parameter to receive only the selected period.
 *
 * *New in version 2.6.0*
 *
 * @param data.updatedAtLte Returns objects less or equal the specified date.
 *
 * This can be combined with updated_at_gte parameter to receive only the selected period.
 *
 * *New in version 2.6.0*
 *
 * @param data.durationGte Returns objects greater than or equal to the specified values.
 *
 * This can be combined with duration_lte parameter to receive only the selected period.
 *
 * @param data.durationLte Returns objects less than or equal to the specified values.
 *
 * This can be combined with duration_gte parameter to receive only the selected range.
 *
 * @param data.state The value can be repeated to retrieve multiple matching values (OR condition).
 * @param data.pool The value can be repeated to retrieve multiple matching values (OR condition).
 * @param data.queue The value can be repeated to retrieve multiple matching values (OR condition).
 * @param data.executor The value can be repeated to retrieve multiple matching values (OR condition).
 * @returns TaskInstanceCollection Success.
 * @throws ApiError
 */
export const prefetchUseTaskInstanceServiceGetTaskInstances = (
  queryClient: QueryClient,
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
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseTaskInstanceServiceGetTaskInstancesKeyFn({
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
    }),
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
      }),
  });
/**
 * Get a task instance
 * @param data The data for the request.
 * @param data.dagId The DAG ID.
 * @param data.dagRunId The DAG run ID.
 * @param data.taskId The task ID.
 * @returns TaskInstance Success.
 * @throws ApiError
 */
export const prefetchUseTaskInstanceServiceGetTaskInstance = (
  queryClient: QueryClient,
  {
    dagId,
    dagRunId,
    taskId,
  }: {
    dagId: string;
    dagRunId: string;
    taskId: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseTaskInstanceServiceGetTaskInstanceKeyFn({
      dagId,
      dagRunId,
      taskId,
    }),
    queryFn: () =>
      TaskInstanceService.getTaskInstance({ dagId, dagRunId, taskId }),
  });
/**
 * Get a mapped task instance
 * Get details of a mapped task instance.
 *
 * *New in version 2.3.0*
 *
 * @param data The data for the request.
 * @param data.dagId The DAG ID.
 * @param data.dagRunId The DAG run ID.
 * @param data.taskId The task ID.
 * @param data.mapIndex The map index.
 * @returns TaskInstance Success.
 * @throws ApiError
 */
export const prefetchUseTaskInstanceServiceGetMappedTaskInstance = (
  queryClient: QueryClient,
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
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstanceKeyFn({
      dagId,
      dagRunId,
      mapIndex,
      taskId,
    }),
    queryFn: () =>
      TaskInstanceService.getMappedTaskInstance({
        dagId,
        dagRunId,
        mapIndex,
        taskId,
      }),
  });
/**
 * List mapped task instances
 * Get details of all mapped task instances.
 *
 * *New in version 2.3.0*
 *
 * @param data The data for the request.
 * @param data.dagId The DAG ID.
 * @param data.dagRunId The DAG run ID.
 * @param data.taskId The task ID.
 * @param data.limit The numbers of items to return.
 * @param data.offset The number of items to skip before starting to collect the result set.
 * @param data.executionDateGte Returns objects greater or equal to the specified date.
 *
 * This can be combined with execution_date_lte parameter to receive only the selected period.
 *
 * @param data.executionDateLte Returns objects less than or equal to the specified date.
 *
 * This can be combined with execution_date_gte parameter to receive only the selected period.
 *
 * @param data.startDateGte Returns objects greater or equal the specified date.
 *
 * This can be combined with start_date_lte parameter to receive only the selected period.
 *
 * @param data.startDateLte Returns objects less or equal the specified date.
 *
 * This can be combined with start_date_gte parameter to receive only the selected period.
 *
 * @param data.endDateGte Returns objects greater or equal the specified date.
 *
 * This can be combined with start_date_lte parameter to receive only the selected period.
 *
 * @param data.endDateLte Returns objects less than or equal to the specified date.
 *
 * This can be combined with start_date_gte parameter to receive only the selected period.
 *
 * @param data.updatedAtGte Returns objects greater or equal the specified date.
 *
 * This can be combined with updated_at_lte parameter to receive only the selected period.
 *
 * *New in version 2.6.0*
 *
 * @param data.updatedAtLte Returns objects less or equal the specified date.
 *
 * This can be combined with updated_at_gte parameter to receive only the selected period.
 *
 * *New in version 2.6.0*
 *
 * @param data.durationGte Returns objects greater than or equal to the specified values.
 *
 * This can be combined with duration_lte parameter to receive only the selected period.
 *
 * @param data.durationLte Returns objects less than or equal to the specified values.
 *
 * This can be combined with duration_gte parameter to receive only the selected range.
 *
 * @param data.state The value can be repeated to retrieve multiple matching values (OR condition).
 * @param data.pool The value can be repeated to retrieve multiple matching values (OR condition).
 * @param data.queue The value can be repeated to retrieve multiple matching values (OR condition).
 * @param data.executor The value can be repeated to retrieve multiple matching values (OR condition).
 * @param data.orderBy The name of the field to order the results by. Prefix a field name
 * with `-` to reverse the sort order. `order_by` defaults to
 * `map_index` when unspecified.
 * Supported field names: `state`, `duration`, `start_date`, `end_date`
 * and `map_index`.
 *
 * *New in version 3.0.0*
 *
 * @returns TaskInstanceCollection Success.
 * @throws ApiError
 */
export const prefetchUseTaskInstanceServiceGetMappedTaskInstances = (
  queryClient: QueryClient,
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
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstancesKeyFn({
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
    }),
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
      }),
  });
/**
 * get taskinstance try
 * Get details of a task instance try.
 *
 * *New in version 2.10.0*
 *
 * @param data The data for the request.
 * @param data.dagId The DAG ID.
 * @param data.dagRunId The DAG run ID.
 * @param data.taskId The task ID.
 * @param data.taskTryNumber The task try number.
 * @returns TaskInstance Success.
 * @throws ApiError
 */
export const prefetchUseTaskInstanceServiceGetTaskInstanceTryDetails = (
  queryClient: QueryClient,
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
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseTaskInstanceServiceGetTaskInstanceTryDetailsKeyFn({
      dagId,
      dagRunId,
      taskId,
      taskTryNumber,
    }),
    queryFn: () =>
      TaskInstanceService.getTaskInstanceTryDetails({
        dagId,
        dagRunId,
        taskId,
        taskTryNumber,
      }),
  });
/**
 * List task instance tries
 * Get details of all task instance tries.
 *
 * *New in version 2.10.0*
 *
 * @param data The data for the request.
 * @param data.dagId The DAG ID.
 * @param data.dagRunId The DAG run ID.
 * @param data.taskId The task ID.
 * @param data.limit The numbers of items to return.
 * @param data.offset The number of items to skip before starting to collect the result set.
 * @param data.orderBy The name of the field to order the results by.
 * Prefix a field name with `-` to reverse the sort order.
 *
 * *New in version 2.1.0*
 *
 * @returns TaskInstanceCollection Success.
 * @throws ApiError
 */
export const prefetchUseTaskInstanceServiceGetTaskInstanceTries = (
  queryClient: QueryClient,
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
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseTaskInstanceServiceGetTaskInstanceTriesKeyFn({
      dagId,
      dagRunId,
      limit,
      offset,
      orderBy,
      taskId,
    }),
    queryFn: () =>
      TaskInstanceService.getTaskInstanceTries({
        dagId,
        dagRunId,
        limit,
        offset,
        orderBy,
        taskId,
      }),
  });
/**
 * List mapped task instance tries
 * Get details of all task instance tries.
 *
 * *New in version 2.10.0*
 *
 * @param data The data for the request.
 * @param data.dagId The DAG ID.
 * @param data.dagRunId The DAG run ID.
 * @param data.taskId The task ID.
 * @param data.mapIndex The map index.
 * @param data.limit The numbers of items to return.
 * @param data.offset The number of items to skip before starting to collect the result set.
 * @param data.orderBy The name of the field to order the results by.
 * Prefix a field name with `-` to reverse the sort order.
 *
 * *New in version 2.1.0*
 *
 * @returns TaskInstanceCollection Success.
 * @throws ApiError
 */
export const prefetchUseTaskInstanceServiceGetMappedTaskInstanceTries = (
  queryClient: QueryClient,
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
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstanceTriesKeyFn({
      dagId,
      dagRunId,
      limit,
      mapIndex,
      offset,
      orderBy,
      taskId,
    }),
    queryFn: () =>
      TaskInstanceService.getMappedTaskInstanceTries({
        dagId,
        dagRunId,
        limit,
        mapIndex,
        offset,
        orderBy,
        taskId,
      }),
  });
/**
 * get mapped taskinstance try
 * Get details of a mapped task instance try.
 *
 * *New in version 2.10.0*
 *
 * @param data The data for the request.
 * @param data.dagId The DAG ID.
 * @param data.dagRunId The DAG run ID.
 * @param data.taskId The task ID.
 * @param data.mapIndex The map index.
 * @param data.taskTryNumber The task try number.
 * @returns TaskInstance Success.
 * @throws ApiError
 */
export const prefetchUseTaskInstanceServiceGetMappedTaskInstanceTryDetails = (
  queryClient: QueryClient,
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
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstanceTryDetailsKeyFn(
      { dagId, dagRunId, mapIndex, taskId, taskTryNumber },
    ),
    queryFn: () =>
      TaskInstanceService.getMappedTaskInstanceTryDetails({
        dagId,
        dagRunId,
        mapIndex,
        taskId,
        taskTryNumber,
      }),
  });
/**
 * List extra links
 * List extra links for task instance.
 *
 * @param data The data for the request.
 * @param data.dagId The DAG ID.
 * @param data.dagRunId The DAG run ID.
 * @param data.taskId The task ID.
 * @returns ExtraLinkCollection Success.
 * @throws ApiError
 */
export const prefetchUseTaskInstanceServiceGetExtraLinks = (
  queryClient: QueryClient,
  {
    dagId,
    dagRunId,
    taskId,
  }: {
    dagId: string;
    dagRunId: string;
    taskId: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseTaskInstanceServiceGetExtraLinksKeyFn({
      dagId,
      dagRunId,
      taskId,
    }),
    queryFn: () =>
      TaskInstanceService.getExtraLinks({ dagId, dagRunId, taskId }),
  });
/**
 * Get logs
 * Get logs for a specific task instance and its try number.
 * To get log from specific character position, following way of using
 * URLSafeSerializer can be used.
 *
 * Example:
 * ```
 * from itsdangerous.url_safe import URLSafeSerializer
 *
 * request_url = f"api/v1/dags/{DAG_ID}/dagRuns/{RUN_ID}/taskInstances/{TASK_ID}/logs/1"
 * key = app.config["SECRET_KEY"]
 * serializer = URLSafeSerializer(key)
 * token = serializer.dumps({"log_pos": 10000})
 *
 * response = self.client.get(
 * request_url,
 * query_string={"token": token},
 * headers={"Accept": "text/plain"},
 * environ_overrides={"REMOTE_USER": "test"},
 * )
 * continuation_token = response.json["continuation_token"]
 * metadata = URLSafeSerializer(key).loads(continuation_token)
 * log_pos = metadata["log_pos"]
 * end_of_log = metadata["end_of_log"]
 * ```
 * If log_pos is passed as 10000 like the above example, it renders the logs starting
 * from char position 10000 to last (not the end as the logs may be tailing behind in
 * running state). This way pagination can be done with metadata as part of the token.
 *
 * @param data The data for the request.
 * @param data.dagId The DAG ID.
 * @param data.dagRunId The DAG run ID.
 * @param data.taskId The task ID.
 * @param data.taskTryNumber The task try number.
 * @param data.fullContent A full content will be returned.
 * By default, only the first fragment will be returned.
 *
 * @param data.mapIndex Filter on map index for mapped task.
 * @param data.token A token that allows you to continue fetching logs.
 * If passed, it will specify the location from which the download should be continued.
 *
 * @returns unknown Success.
 * @throws ApiError
 */
export const prefetchUseTaskInstanceServiceGetLog = (
  queryClient: QueryClient,
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
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseTaskInstanceServiceGetLogKeyFn({
      dagId,
      dagRunId,
      fullContent,
      mapIndex,
      taskId,
      taskTryNumber,
      token,
    }),
    queryFn: () =>
      TaskInstanceService.getLog({
        dagId,
        dagRunId,
        fullContent,
        mapIndex,
        taskId,
        taskTryNumber,
        token,
      }),
  });
/**
 * List DAG runs
 * This endpoint allows specifying `~` as the dag_id to retrieve DAG runs for all DAGs.
 *
 * @param data The data for the request.
 * @param data.dagId The DAG ID.
 * @param data.limit The numbers of items to return.
 * @param data.offset The number of items to skip before starting to collect the result set.
 * @param data.executionDateGte Returns objects greater or equal to the specified date.
 *
 * This can be combined with execution_date_lte parameter to receive only the selected period.
 *
 * @param data.executionDateLte Returns objects less than or equal to the specified date.
 *
 * This can be combined with execution_date_gte parameter to receive only the selected period.
 *
 * @param data.startDateGte Returns objects greater or equal the specified date.
 *
 * This can be combined with start_date_lte parameter to receive only the selected period.
 *
 * @param data.startDateLte Returns objects less or equal the specified date.
 *
 * This can be combined with start_date_gte parameter to receive only the selected period.
 *
 * @param data.endDateGte Returns objects greater or equal the specified date.
 *
 * This can be combined with start_date_lte parameter to receive only the selected period.
 *
 * @param data.endDateLte Returns objects less than or equal to the specified date.
 *
 * This can be combined with start_date_gte parameter to receive only the selected period.
 *
 * @param data.updatedAtGte Returns objects greater or equal the specified date.
 *
 * This can be combined with updated_at_lte parameter to receive only the selected period.
 *
 * *New in version 2.6.0*
 *
 * @param data.updatedAtLte Returns objects less or equal the specified date.
 *
 * This can be combined with updated_at_gte parameter to receive only the selected period.
 *
 * *New in version 2.6.0*
 *
 * @param data.state The value can be repeated to retrieve multiple matching values (OR condition).
 * @param data.orderBy The name of the field to order the results by.
 * Prefix a field name with `-` to reverse the sort order.
 *
 * *New in version 2.1.0*
 *
 * @param data.fields List of field for return.
 *
 * @returns DAGRunCollection List of DAG runs.
 * @throws ApiError
 */
export const prefetchUseDagRunServiceGetDagRuns = (
  queryClient: QueryClient,
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
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagRunServiceGetDagRunsKeyFn({
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
    }),
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
      }),
  });
/**
 * Get a DAG run
 * @param data The data for the request.
 * @param data.dagId The DAG ID.
 * @param data.dagRunId The DAG run ID.
 * @param data.fields List of field for return.
 *
 * @returns DAGRun Success.
 * @throws ApiError
 */
export const prefetchUseDagRunServiceGetDagRun = (
  queryClient: QueryClient,
  {
    dagId,
    dagRunId,
    fields,
  }: {
    dagId: string;
    dagRunId: string;
    fields?: string[];
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagRunServiceGetDagRunKeyFn({
      dagId,
      dagRunId,
      fields,
    }),
    queryFn: () => DagRunService.getDagRun({ dagId, dagRunId, fields }),
  });
/**
 * Get dataset events for a DAG run
 * Get datasets for a dag run.
 *
 * *New in version 2.4.0*
 *
 * @param data The data for the request.
 * @param data.dagId The DAG ID.
 * @param data.dagRunId The DAG run ID.
 * @returns DatasetEventCollection Success.
 * @throws ApiError
 */
export const prefetchUseDagRunServiceGetUpstreamDatasetEvents = (
  queryClient: QueryClient,
  {
    dagId,
    dagRunId,
  }: {
    dagId: string;
    dagRunId: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagRunServiceGetUpstreamDatasetEventsKeyFn({
      dagId,
      dagRunId,
    }),
    queryFn: () => DagRunService.getUpstreamDatasetEvents({ dagId, dagRunId }),
  });
/**
 * Get dataset events for a DAG run
 * Get datasets for a dag run.
 *
 * *New in version 2.4.0*
 *
 * @param data The data for the request.
 * @param data.dagId The DAG ID.
 * @param data.dagRunId The DAG run ID.
 * @returns DatasetEventCollection Success.
 * @throws ApiError
 */
export const prefetchUseDatasetServiceGetUpstreamDatasetEvents = (
  queryClient: QueryClient,
  {
    dagId,
    dagRunId,
  }: {
    dagId: string;
    dagRunId: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDatasetServiceGetUpstreamDatasetEventsKeyFn({
      dagId,
      dagRunId,
    }),
    queryFn: () => DatasetService.getUpstreamDatasetEvents({ dagId, dagRunId }),
  });
/**
 * Get a queued Dataset event for a DAG
 * Get a queued Dataset event for a DAG.
 *
 * *New in version 2.9.0*
 *
 * @param data The data for the request.
 * @param data.dagId The DAG ID.
 * @param data.uri The encoded Dataset URI
 * @param data.before Timestamp to select event logs occurring before.
 * @returns QueuedEvent Success.
 * @throws ApiError
 */
export const prefetchUseDatasetServiceGetDagDatasetQueuedEvent = (
  queryClient: QueryClient,
  {
    before,
    dagId,
    uri,
  }: {
    before?: string;
    dagId: string;
    uri: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDatasetServiceGetDagDatasetQueuedEventKeyFn({
      before,
      dagId,
      uri,
    }),
    queryFn: () =>
      DatasetService.getDagDatasetQueuedEvent({ before, dagId, uri }),
  });
/**
 * Get queued Dataset events for a DAG.
 * Get queued Dataset events for a DAG.
 *
 * *New in version 2.9.0*
 *
 * @param data The data for the request.
 * @param data.dagId The DAG ID.
 * @param data.before Timestamp to select event logs occurring before.
 * @returns QueuedEventCollection Success.
 * @throws ApiError
 */
export const prefetchUseDatasetServiceGetDagDatasetQueuedEvents = (
  queryClient: QueryClient,
  {
    before,
    dagId,
  }: {
    before?: string;
    dagId: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDatasetServiceGetDagDatasetQueuedEventsKeyFn({
      before,
      dagId,
    }),
    queryFn: () => DatasetService.getDagDatasetQueuedEvents({ before, dagId }),
  });
/**
 * Get queued Dataset events for a Dataset.
 * Get queued Dataset events for a Dataset
 *
 * *New in version 2.9.0*
 *
 * @param data The data for the request.
 * @param data.uri The encoded Dataset URI
 * @param data.before Timestamp to select event logs occurring before.
 * @returns QueuedEventCollection Success.
 * @throws ApiError
 */
export const prefetchUseDatasetServiceGetDatasetQueuedEvents = (
  queryClient: QueryClient,
  {
    before,
    uri,
  }: {
    before?: string;
    uri: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDatasetServiceGetDatasetQueuedEventsKeyFn({
      before,
      uri,
    }),
    queryFn: () => DatasetService.getDatasetQueuedEvents({ before, uri }),
  });
/**
 * List datasets
 * @param data The data for the request.
 * @param data.limit The numbers of items to return.
 * @param data.offset The number of items to skip before starting to collect the result set.
 * @param data.orderBy The name of the field to order the results by.
 * Prefix a field name with `-` to reverse the sort order.
 *
 * *New in version 2.1.0*
 *
 * @param data.uriPattern If set, only return datasets with uris matching this pattern.
 *
 * @param data.dagIds One or more DAG IDs separated by commas to filter datasets by associated DAGs either consuming or producing.
 *
 * *New in version 2.9.0*
 *
 * @returns DatasetCollection Success.
 * @throws ApiError
 */
export const prefetchUseDatasetServiceGetDatasets = (
  queryClient: QueryClient,
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
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDatasetServiceGetDatasetsKeyFn({
      dagIds,
      limit,
      offset,
      orderBy,
      uriPattern,
    }),
    queryFn: () =>
      DatasetService.getDatasets({
        dagIds,
        limit,
        offset,
        orderBy,
        uriPattern,
      }),
  });
/**
 * Get a dataset
 * Get a dataset by uri.
 * @param data The data for the request.
 * @param data.uri The encoded Dataset URI
 * @returns Dataset Success.
 * @throws ApiError
 */
export const prefetchUseDatasetServiceGetDataset = (
  queryClient: QueryClient,
  {
    uri,
  }: {
    uri: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDatasetServiceGetDatasetKeyFn({ uri }),
    queryFn: () => DatasetService.getDataset({ uri }),
  });
/**
 * Get dataset events
 * Get dataset events
 * @param data The data for the request.
 * @param data.limit The numbers of items to return.
 * @param data.offset The number of items to skip before starting to collect the result set.
 * @param data.orderBy The name of the field to order the results by.
 * Prefix a field name with `-` to reverse the sort order.
 *
 * *New in version 2.1.0*
 *
 * @param data.datasetId The Dataset ID that updated the dataset.
 * @param data.sourceDagId The DAG ID that updated the dataset.
 * @param data.sourceTaskId The task ID that updated the dataset.
 * @param data.sourceRunId The DAG run ID that updated the dataset.
 * @param data.sourceMapIndex The map index that updated the dataset.
 * @returns DatasetEventCollection Success.
 * @throws ApiError
 */
export const prefetchUseDatasetServiceGetDatasetEvents = (
  queryClient: QueryClient,
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
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDatasetServiceGetDatasetEventsKeyFn({
      datasetId,
      limit,
      offset,
      orderBy,
      sourceDagId,
      sourceMapIndex,
      sourceRunId,
      sourceTaskId,
    }),
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
      }),
  });
/**
 * List log entries
 * List log entries from event log.
 * @param data The data for the request.
 * @param data.limit The numbers of items to return.
 * @param data.offset The number of items to skip before starting to collect the result set.
 * @param data.orderBy The name of the field to order the results by.
 * Prefix a field name with `-` to reverse the sort order.
 *
 * *New in version 2.1.0*
 *
 * @param data.dagId Returns objects matched by the DAG ID.
 * @param data.taskId Returns objects matched by the Task ID.
 * @param data.runId Returns objects matched by the Run ID.
 * @param data.mapIndex Filter on map index for mapped task.
 * @param data.tryNumber Filter on try_number for task instance.
 * @param data.event The name of event log.
 * @param data.owner The owner's name of event log.
 * @param data.before Timestamp to select event logs occurring before.
 * @param data.after Timestamp to select event logs occurring after.
 * @param data.includedEvents One or more event names separated by commas. If set, only return event logs with events matching this pattern.
 * *New in version 2.9.0*
 *
 * @param data.excludedEvents One or more event names separated by commas. If set, only return event logs with events that do not match this pattern.
 * *New in version 2.9.0*
 *
 * @returns EventLogCollection Success.
 * @throws ApiError
 */
export const prefetchUseEventLogServiceGetEventLogs = (
  queryClient: QueryClient,
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
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseEventLogServiceGetEventLogsKeyFn({
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
    }),
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
      }),
  });
/**
 * Get a log entry
 * @param data The data for the request.
 * @param data.eventLogId The event log ID.
 * @returns EventLog Success.
 * @throws ApiError
 */
export const prefetchUseEventLogServiceGetEventLog = (
  queryClient: QueryClient,
  {
    eventLogId,
  }: {
    eventLogId: number;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseEventLogServiceGetEventLogKeyFn({ eventLogId }),
    queryFn: () => EventLogService.getEventLog({ eventLogId }),
  });
/**
 * List import errors
 * @param data The data for the request.
 * @param data.limit The numbers of items to return.
 * @param data.offset The number of items to skip before starting to collect the result set.
 * @param data.orderBy The name of the field to order the results by.
 * Prefix a field name with `-` to reverse the sort order.
 *
 * *New in version 2.1.0*
 *
 * @returns ImportErrorCollection Success.
 * @throws ApiError
 */
export const prefetchUseImportErrorServiceGetImportErrors = (
  queryClient: QueryClient,
  {
    limit,
    offset,
    orderBy,
  }: {
    limit?: number;
    offset?: number;
    orderBy?: string;
  } = {},
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseImportErrorServiceGetImportErrorsKeyFn({
      limit,
      offset,
      orderBy,
    }),
    queryFn: () =>
      ImportErrorService.getImportErrors({ limit, offset, orderBy }),
  });
/**
 * Get an import error
 * @param data The data for the request.
 * @param data.importErrorId The import error ID.
 * @returns ImportError Success.
 * @throws ApiError
 */
export const prefetchUseImportErrorServiceGetImportError = (
  queryClient: QueryClient,
  {
    importErrorId,
  }: {
    importErrorId: number;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseImportErrorServiceGetImportErrorKeyFn({
      importErrorId,
    }),
    queryFn: () => ImportErrorService.getImportError({ importErrorId }),
  });
/**
 * List pools
 * @param data The data for the request.
 * @param data.limit The numbers of items to return.
 * @param data.offset The number of items to skip before starting to collect the result set.
 * @param data.orderBy The name of the field to order the results by.
 * Prefix a field name with `-` to reverse the sort order.
 *
 * *New in version 2.1.0*
 *
 * @returns PoolCollection List of pools.
 * @throws ApiError
 */
export const prefetchUsePoolServiceGetPools = (
  queryClient: QueryClient,
  {
    limit,
    offset,
    orderBy,
  }: {
    limit?: number;
    offset?: number;
    orderBy?: string;
  } = {},
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UsePoolServiceGetPoolsKeyFn({ limit, offset, orderBy }),
    queryFn: () => PoolService.getPools({ limit, offset, orderBy }),
  });
/**
 * Get a pool
 * @param data The data for the request.
 * @param data.poolName The pool name.
 * @returns Pool Success.
 * @throws ApiError
 */
export const prefetchUsePoolServiceGetPool = (
  queryClient: QueryClient,
  {
    poolName,
  }: {
    poolName: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UsePoolServiceGetPoolKeyFn({ poolName }),
    queryFn: () => PoolService.getPool({ poolName }),
  });
/**
 * List providers
 * Get a list of providers.
 *
 * *New in version 2.1.0*
 *
 * @returns unknown List of providers.
 * @throws ApiError
 */
export const prefetchUseProviderServiceGetProviders = (
  queryClient: QueryClient,
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseProviderServiceGetProvidersKeyFn(),
    queryFn: () => ProviderService.getProviders(),
  });
/**
 * List variables
 * The collection does not contain data. To get data, you must get a single entity.
 * @param data The data for the request.
 * @param data.limit The numbers of items to return.
 * @param data.offset The number of items to skip before starting to collect the result set.
 * @param data.orderBy The name of the field to order the results by.
 * Prefix a field name with `-` to reverse the sort order.
 *
 * *New in version 2.1.0*
 *
 * @returns VariableCollection Success.
 * @throws ApiError
 */
export const prefetchUseVariableServiceGetVariables = (
  queryClient: QueryClient,
  {
    limit,
    offset,
    orderBy,
  }: {
    limit?: number;
    offset?: number;
    orderBy?: string;
  } = {},
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseVariableServiceGetVariablesKeyFn({
      limit,
      offset,
      orderBy,
    }),
    queryFn: () => VariableService.getVariables({ limit, offset, orderBy }),
  });
/**
 * Get a variable
 * Get a variable by key.
 * @param data The data for the request.
 * @param data.variableKey The variable Key.
 * @returns Variable Success.
 * @throws ApiError
 */
export const prefetchUseVariableServiceGetVariable = (
  queryClient: QueryClient,
  {
    variableKey,
  }: {
    variableKey: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseVariableServiceGetVariableKeyFn({ variableKey }),
    queryFn: () => VariableService.getVariable({ variableKey }),
  });
/**
 * List XCom entries
 * This endpoint allows specifying `~` as the dag_id, dag_run_id, task_id to retrieve XCOM entries for for all DAGs, DAG runs and task instances. XCom values won't be returned as they can be large. Use this endpoint to get a list of XCom entries and then fetch individual entry to get value.
 * @param data The data for the request.
 * @param data.dagId The DAG ID.
 * @param data.dagRunId The DAG run ID.
 * @param data.taskId The task ID.
 * @param data.mapIndex Filter on map index for mapped task.
 * @param data.xcomKey Only filter the XCom records which have the provided key.
 * @param data.limit The numbers of items to return.
 * @param data.offset The number of items to skip before starting to collect the result set.
 * @returns XComCollection Success.
 * @throws ApiError
 */
export const prefetchUseXcomServiceGetXcomEntries = (
  queryClient: QueryClient,
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
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseXcomServiceGetXcomEntriesKeyFn({
      dagId,
      dagRunId,
      limit,
      mapIndex,
      offset,
      taskId,
      xcomKey,
    }),
    queryFn: () =>
      XcomService.getXcomEntries({
        dagId,
        dagRunId,
        limit,
        mapIndex,
        offset,
        taskId,
        xcomKey,
      }),
  });
/**
 * Get an XCom entry
 * @param data The data for the request.
 * @param data.dagId The DAG ID.
 * @param data.dagRunId The DAG run ID.
 * @param data.taskId The task ID.
 * @param data.xcomKey The XCom key.
 * @param data.mapIndex Filter on map index for mapped task.
 * @param data.deserialize Whether to deserialize an XCom value when using a custom XCom backend.
 *
 * The XCom API endpoint calls `orm_deserialize_value` by default since an XCom may contain value
 * that is potentially expensive to deserialize in the web server. Setting this to true overrides
 * the consideration, and calls `deserialize_value` instead.
 *
 * This parameter is not meaningful when using the default XCom backend.
 *
 * *New in version 2.4.0*
 *
 * @param data.stringify Whether to convert the XCom value to be a string. XCom values can be of Any data type.
 *
 * If set to true (default) the Any value will be returned as string, e.g. a Python representation
 * of a dict. If set to false it will return the raw data as dict, list, string or whatever was stored.
 *
 * *New in version 2.10.0*
 *
 * @returns XCom Success.
 * @throws ApiError
 */
export const prefetchUseXcomServiceGetXcomEntry = (
  queryClient: QueryClient,
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
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseXcomServiceGetXcomEntryKeyFn({
      dagId,
      dagRunId,
      deserialize,
      mapIndex,
      stringify,
      taskId,
      xcomKey,
    }),
    queryFn: () =>
      XcomService.getXcomEntry({
        dagId,
        dagRunId,
        deserialize,
        mapIndex,
        stringify,
        taskId,
        xcomKey,
      }),
  });
/**
 * List Dag statistics
 * @param data The data for the request.
 * @param data.dagIds One or more DAG IDs separated by commas to filter relevant Dags.
 *
 * @returns DagStatsCollectionSchema Success.
 * @throws ApiError
 */
export const prefetchUseDagStatsServiceGetDagStats = (
  queryClient: QueryClient,
  {
    dagIds,
  }: {
    dagIds: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagStatsServiceGetDagStatsKeyFn({ dagIds }),
    queryFn: () => DagStatsService.getDagStats({ dagIds }),
  });
/**
 * List dag warnings
 * @param data The data for the request.
 * @param data.dagId If set, only return DAG warnings with this dag_id.
 * @param data.warningType If set, only return DAG warnings with this type.
 * @param data.limit The numbers of items to return.
 * @param data.offset The number of items to skip before starting to collect the result set.
 * @param data.orderBy The name of the field to order the results by.
 * Prefix a field name with `-` to reverse the sort order.
 *
 * *New in version 2.1.0*
 *
 * @returns DagWarningCollection Success.
 * @throws ApiError
 */
export const prefetchUseDagWarningServiceGetDagWarnings = (
  queryClient: QueryClient,
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
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagWarningServiceGetDagWarningsKeyFn({
      dagId,
      limit,
      offset,
      orderBy,
      warningType,
    }),
    queryFn: () =>
      DagWarningService.getDagWarnings({
        dagId,
        limit,
        offset,
        orderBy,
        warningType,
      }),
  });
/**
 * Get current configuration
 * @param data The data for the request.
 * @param data.section If given, only return config of this section.
 * @returns Config Success.
 * @throws ApiError
 */
export const prefetchUseConfigServiceGetConfig = (
  queryClient: QueryClient,
  {
    section,
  }: {
    section?: string;
  } = {},
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseConfigServiceGetConfigKeyFn({ section }),
    queryFn: () => ConfigService.getConfig({ section }),
  });
/**
 * Get a option from configuration
 * @param data The data for the request.
 * @param data.section
 * @param data.option
 * @returns Config Success.
 * @throws ApiError
 */
export const prefetchUseConfigServiceGetValue = (
  queryClient: QueryClient,
  {
    option,
    section,
  }: {
    option: string;
    section: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseConfigServiceGetValueKeyFn({ option, section }),
    queryFn: () => ConfigService.getValue({ option, section }),
  });
/**
 * Get instance status
 * Get the status of Airflow's metadatabase, triggerer and scheduler. It includes info about
 * metadatabase and last heartbeat of scheduler and triggerer.
 *
 * @returns HealthInfo Success.
 * @throws ApiError
 */
export const prefetchUseMonitoringServiceGetHealth = (
  queryClient: QueryClient,
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseMonitoringServiceGetHealthKeyFn(),
    queryFn: () => MonitoringService.getHealth(),
  });
/**
 * Get version information
 * @returns VersionInfo Success.
 * @throws ApiError
 */
export const prefetchUseMonitoringServiceGetVersion = (
  queryClient: QueryClient,
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseMonitoringServiceGetVersionKeyFn(),
    queryFn: () => MonitoringService.getVersion(),
  });
/**
 * Get a list of loaded plugins
 * Get a list of loaded plugins.
 *
 * *New in version 2.1.0*
 *
 * @param data The data for the request.
 * @param data.limit The numbers of items to return.
 * @param data.offset The number of items to skip before starting to collect the result set.
 * @returns PluginCollection Success
 * @throws ApiError
 */
export const prefetchUsePluginServiceGetPlugins = (
  queryClient: QueryClient,
  {
    limit,
    offset,
  }: {
    limit?: number;
    offset?: number;
  } = {},
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UsePluginServiceGetPluginsKeyFn({ limit, offset }),
    queryFn: () => PluginService.getPlugins({ limit, offset }),
  });
/**
 * @deprecated
 * List roles
 * Get a list of roles.
 *
 * *This API endpoint is deprecated, please use the endpoint `/auth/fab/v1` for this operation instead.*
 *
 * @param data The data for the request.
 * @param data.limit The numbers of items to return.
 * @param data.offset The number of items to skip before starting to collect the result set.
 * @param data.orderBy The name of the field to order the results by.
 * Prefix a field name with `-` to reverse the sort order.
 *
 * *New in version 2.1.0*
 *
 * @returns RoleCollection Success.
 * @throws ApiError
 */
export const prefetchUseRoleServiceGetRoles = (
  queryClient: QueryClient,
  {
    limit,
    offset,
    orderBy,
  }: {
    limit?: number;
    offset?: number;
    orderBy?: string;
  } = {},
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseRoleServiceGetRolesKeyFn({ limit, offset, orderBy }),
    queryFn: () => RoleService.getRoles({ limit, offset, orderBy }),
  });
/**
 * @deprecated
 * Get a role
 * Get a role.
 *
 * *This API endpoint is deprecated, please use the endpoint `/auth/fab/v1` for this operation instead.*
 *
 * @param data The data for the request.
 * @param data.roleName The role name
 * @returns Role Success.
 * @throws ApiError
 */
export const prefetchUseRoleServiceGetRole = (
  queryClient: QueryClient,
  {
    roleName,
  }: {
    roleName: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseRoleServiceGetRoleKeyFn({ roleName }),
    queryFn: () => RoleService.getRole({ roleName }),
  });
/**
 * @deprecated
 * List permissions
 * Get a list of permissions.
 *
 * *This API endpoint is deprecated, please use the endpoint `/auth/fab/v1` for this operation instead.*
 *
 * @param data The data for the request.
 * @param data.limit The numbers of items to return.
 * @param data.offset The number of items to skip before starting to collect the result set.
 * @returns ActionCollection Success.
 * @throws ApiError
 */
export const prefetchUsePermissionServiceGetPermissions = (
  queryClient: QueryClient,
  {
    limit,
    offset,
  }: {
    limit?: number;
    offset?: number;
  } = {},
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UsePermissionServiceGetPermissionsKeyFn({ limit, offset }),
    queryFn: () => PermissionService.getPermissions({ limit, offset }),
  });
/**
 * @deprecated
 * List users
 * Get a list of users.
 *
 * *This API endpoint is deprecated, please use the endpoint `/auth/fab/v1` for this operation instead.*
 *
 * @param data The data for the request.
 * @param data.limit The numbers of items to return.
 * @param data.offset The number of items to skip before starting to collect the result set.
 * @param data.orderBy The name of the field to order the results by.
 * Prefix a field name with `-` to reverse the sort order.
 *
 * *New in version 2.1.0*
 *
 * @returns UserCollection Success.
 * @throws ApiError
 */
export const prefetchUseUserServiceGetUsers = (
  queryClient: QueryClient,
  {
    limit,
    offset,
    orderBy,
  }: {
    limit?: number;
    offset?: number;
    orderBy?: string;
  } = {},
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseUserServiceGetUsersKeyFn({ limit, offset, orderBy }),
    queryFn: () => UserService.getUsers({ limit, offset, orderBy }),
  });
/**
 * @deprecated
 * Get a user
 * Get a user with a specific username.
 *
 * *This API endpoint is deprecated, please use the endpoint `/auth/fab/v1` for this operation instead.*
 *
 * @param data The data for the request.
 * @param data.username The username of the user.
 *
 * *New in version 2.1.0*
 *
 * @returns UserCollectionItem Success.
 * @throws ApiError
 */
export const prefetchUseUserServiceGetUser = (
  queryClient: QueryClient,
  {
    username,
  }: {
    username: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseUserServiceGetUserKeyFn({ username }),
    queryFn: () => UserService.getUser({ username }),
  });
