// generated with @7nohe/openapi-react-query-codegen@1.6.0
import { type QueryClient } from "@tanstack/react-query";

import {
  AssetService,
  BackfillService,
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
import * as Common from "./common";

/**
 * Next Run Assets
 * @param data The data for the request.
 * @param data.dagId
 * @returns unknown Successful Response
 * @throws ApiError
 */
export const prefetchUseAssetServiceNextRunAssets = (
  queryClient: QueryClient,
  {
    dagId,
  }: {
    dagId: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseAssetServiceNextRunAssetsKeyFn({ dagId }),
    queryFn: () => AssetService.nextRunAssets({ dagId }),
  });
/**
 * Get Assets
 * Get assets.
 * @param data The data for the request.
 * @param data.limit
 * @param data.offset
 * @param data.uriPattern
 * @param data.dagIds
 * @param data.orderBy
 * @returns AssetCollectionResponse Successful Response
 * @throws ApiError
 */
export const prefetchUseAssetServiceGetAssets = (
  queryClient: QueryClient,
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
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseAssetServiceGetAssetsKeyFn({
      dagIds,
      limit,
      offset,
      orderBy,
      uriPattern,
    }),
    queryFn: () =>
      AssetService.getAssets({ dagIds, limit, offset, orderBy, uriPattern }),
  });
/**
 * Historical Metrics
 * Return cluster activity historical metrics.
 * @param data The data for the request.
 * @param data.startDate
 * @param data.endDate
 * @returns HistoricalMetricDataResponse Successful Response
 * @throws ApiError
 */
export const prefetchUseDashboardServiceHistoricalMetrics = (
  queryClient: QueryClient,
  {
    endDate,
    startDate,
  }: {
    endDate: string;
    startDate: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDashboardServiceHistoricalMetricsKeyFn({
      endDate,
      startDate,
    }),
    queryFn: () => DashboardService.historicalMetrics({ endDate, startDate }),
  });
/**
 * Recent Dag Runs
 * Get recent DAG runs.
 * @param data The data for the request.
 * @param data.dagRunsLimit
 * @param data.limit
 * @param data.offset
 * @param data.tags
 * @param data.owners
 * @param data.dagIdPattern
 * @param data.dagDisplayNamePattern
 * @param data.onlyActive
 * @param data.paused
 * @param data.lastDagRunState
 * @returns DAGWithLatestDagRunsCollectionResponse Successful Response
 * @throws ApiError
 */
export const prefetchUseDagsServiceRecentDagRuns = (
  queryClient: QueryClient,
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
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagsServiceRecentDagRunsKeyFn({
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
    }),
    queryFn: () =>
      DagsService.recentDagRuns({
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
      }),
  });
/**
 * List Backfills
 * @param data The data for the request.
 * @param data.dagId
 * @param data.limit
 * @param data.offset
 * @param data.orderBy
 * @returns unknown Successful Response
 * @throws ApiError
 */
export const prefetchUseBackfillServiceListBackfills = (
  queryClient: QueryClient,
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
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseBackfillServiceListBackfillsKeyFn({
      dagId,
      limit,
      offset,
      orderBy,
    }),
    queryFn: () =>
      BackfillService.listBackfills({ dagId, limit, offset, orderBy }),
  });
/**
 * Get Backfill
 * @param data The data for the request.
 * @param data.backfillId
 * @returns unknown Successful Response
 * @throws ApiError
 */
export const prefetchUseBackfillServiceGetBackfill = (
  queryClient: QueryClient,
  {
    backfillId,
  }: {
    backfillId: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseBackfillServiceGetBackfillKeyFn({ backfillId }),
    queryFn: () => BackfillService.getBackfill({ backfillId }),
  });
/**
 * Get Connection
 * Get a connection entry.
 * @param data The data for the request.
 * @param data.connectionId
 * @returns ConnectionResponse Successful Response
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
 * Get Connections
 * Get all connection entries.
 * @param data The data for the request.
 * @param data.limit
 * @param data.offset
 * @param data.orderBy
 * @returns ConnectionCollectionResponse Successful Response
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
 * Get Dag Run
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @returns DAGRunResponse Successful Response
 * @throws ApiError
 */
export const prefetchUseDagRunServiceGetDagRun = (
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
    queryKey: Common.UseDagRunServiceGetDagRunKeyFn({ dagId, dagRunId }),
    queryFn: () => DagRunService.getDagRun({ dagId, dagRunId }),
  });
/**
 * Get Dag Source
 * Get source code using file token.
 * @param data The data for the request.
 * @param data.fileToken
 * @param data.accept
 * @returns DAGSourceResponse Successful Response
 * @throws ApiError
 */
export const prefetchUseDagSourceServiceGetDagSource = (
  queryClient: QueryClient,
  {
    accept,
    fileToken,
  }: {
    accept?: string;
    fileToken: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagSourceServiceGetDagSourceKeyFn({
      accept,
      fileToken,
    }),
    queryFn: () => DagSourceService.getDagSource({ accept, fileToken }),
  });
/**
 * Get Dag Stats
 * Get Dag statistics.
 * @param data The data for the request.
 * @param data.dagIds
 * @returns DagStatsCollectionResponse Successful Response
 * @throws ApiError
 */
export const prefetchUseDagStatsServiceGetDagStats = (
  queryClient: QueryClient,
  {
    dagIds,
  }: {
    dagIds?: string[];
  } = {},
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagStatsServiceGetDagStatsKeyFn({ dagIds }),
    queryFn: () => DagStatsService.getDagStats({ dagIds }),
  });
/**
 * List Dag Warnings
 * Get a list of DAG warnings.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.warningType
 * @param data.limit
 * @param data.offset
 * @param data.orderBy
 * @returns DAGWarningCollectionResponse Successful Response
 * @throws ApiError
 */
export const prefetchUseDagWarningServiceListDagWarnings = (
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
    warningType?: DagWarningType;
  } = {},
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagWarningServiceListDagWarningsKeyFn({
      dagId,
      limit,
      offset,
      orderBy,
      warningType,
    }),
    queryFn: () =>
      DagWarningService.listDagWarnings({
        dagId,
        limit,
        offset,
        orderBy,
        warningType,
      }),
  });
/**
 * Get Dags
 * Get all DAGs.
 * @param data The data for the request.
 * @param data.limit
 * @param data.offset
 * @param data.tags
 * @param data.owners
 * @param data.dagIdPattern
 * @param data.dagDisplayNamePattern
 * @param data.onlyActive
 * @param data.paused
 * @param data.lastDagRunState
 * @param data.orderBy
 * @returns DAGCollectionResponse Successful Response
 * @throws ApiError
 */
export const prefetchUseDagServiceGetDags = (
  queryClient: QueryClient,
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
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagServiceGetDagsKeyFn({
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
    }),
    queryFn: () =>
      DagService.getDags({
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
      }),
  });
/**
 * Get Dag Tags
 * Get all DAG tags.
 * @param data The data for the request.
 * @param data.limit
 * @param data.offset
 * @param data.orderBy
 * @param data.tagNamePattern
 * @returns DAGTagCollectionResponse Successful Response
 * @throws ApiError
 */
export const prefetchUseDagServiceGetDagTags = (
  queryClient: QueryClient,
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
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagServiceGetDagTagsKeyFn({
      limit,
      offset,
      orderBy,
      tagNamePattern,
    }),
    queryFn: () =>
      DagService.getDagTags({ limit, offset, orderBy, tagNamePattern }),
  });
/**
 * Get Dag
 * Get basic information about a DAG.
 * @param data The data for the request.
 * @param data.dagId
 * @returns DAGResponse Successful Response
 * @throws ApiError
 */
export const prefetchUseDagServiceGetDag = (
  queryClient: QueryClient,
  {
    dagId,
  }: {
    dagId: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagServiceGetDagKeyFn({ dagId }),
    queryFn: () => DagService.getDag({ dagId }),
  });
/**
 * Get Dag Details
 * Get details of DAG.
 * @param data The data for the request.
 * @param data.dagId
 * @returns DAGDetailsResponse Successful Response
 * @throws ApiError
 */
export const prefetchUseDagServiceGetDagDetails = (
  queryClient: QueryClient,
  {
    dagId,
  }: {
    dagId: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagServiceGetDagDetailsKeyFn({ dagId }),
    queryFn: () => DagService.getDagDetails({ dagId }),
  });
/**
 * Get Event Log
 * @param data The data for the request.
 * @param data.eventLogId
 * @returns EventLogResponse Successful Response
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
 * Get Event Logs
 * Get all Event Logs.
 * @param data The data for the request.
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
 * @param data.limit
 * @param data.offset
 * @param data.orderBy
 * @returns EventLogCollectionResponse Successful Response
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
 * Get Import Error
 * Get an import error.
 * @param data The data for the request.
 * @param data.importErrorId
 * @returns ImportErrorResponse Successful Response
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
 * Get Import Errors
 * Get all import errors.
 * @param data The data for the request.
 * @param data.limit
 * @param data.offset
 * @param data.orderBy
 * @returns ImportErrorCollectionResponse Successful Response
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
 * Get Health
 * @returns HealthInfoSchema Successful Response
 * @throws ApiError
 */
export const prefetchUseMonitorServiceGetHealth = (queryClient: QueryClient) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseMonitorServiceGetHealthKeyFn(),
    queryFn: () => MonitorService.getHealth(),
  });
/**
 * Get Plugins
 * @param data The data for the request.
 * @param data.limit
 * @param data.offset
 * @returns PluginCollectionResponse Successful Response
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
 * Get Pool
 * Get a pool.
 * @param data The data for the request.
 * @param data.poolName
 * @returns PoolResponse Successful Response
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
 * Get Pools
 * Get all pools entries.
 * @param data The data for the request.
 * @param data.limit
 * @param data.offset
 * @param data.orderBy
 * @returns PoolCollectionResponse Successful Response
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
 * Get Providers
 * Get providers.
 * @param data The data for the request.
 * @param data.limit
 * @param data.offset
 * @returns ProviderCollectionResponse Successful Response
 * @throws ApiError
 */
export const prefetchUseProviderServiceGetProviders = (
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
    queryKey: Common.UseProviderServiceGetProvidersKeyFn({ limit, offset }),
    queryFn: () => ProviderService.getProviders({ limit, offset }),
  });
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
 * Get Mapped Task Instances
 * Get list of mapped task instances.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @param data.taskId
 * @param data.logicalDateGte
 * @param data.logicalDateLte
 * @param data.startDateGte
 * @param data.startDateLte
 * @param data.endDateGte
 * @param data.endDateLte
 * @param data.updatedAtGte
 * @param data.updatedAtLte
 * @param data.durationGte
 * @param data.durationLte
 * @param data.state
 * @param data.pool
 * @param data.queue
 * @param data.executor
 * @param data.limit
 * @param data.offset
 * @param data.orderBy
 * @returns TaskInstanceCollectionResponse Successful Response
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
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstancesKeyFn({
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
    }),
    queryFn: () =>
      TaskInstanceService.getMappedTaskInstances({
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
      }),
  });
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
export const prefetchUseTaskInstanceServiceGetTaskInstanceDependencies = (
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
    queryKey: Common.UseTaskInstanceServiceGetTaskInstanceDependenciesKeyFn({
      dagId,
      dagRunId,
      mapIndex,
      taskId,
    }),
    queryFn: () =>
      TaskInstanceService.getTaskInstanceDependencies({
        dagId,
        dagRunId,
        mapIndex,
        taskId,
      }),
  });
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
export const prefetchUseTaskInstanceServiceGetTaskInstanceDependencies1 = (
  queryClient: QueryClient,
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
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseTaskInstanceServiceGetTaskInstanceDependencies1KeyFn({
      dagId,
      dagRunId,
      mapIndex,
      taskId,
    }),
    queryFn: () =>
      TaskInstanceService.getTaskInstanceDependencies1({
        dagId,
        dagRunId,
        mapIndex,
        taskId,
      }),
  });
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
 * Get Task Instances
 * Get list of task instances.
 *
 * This endpoint allows specifying `~` as the dag_id, dag_run_id to retrieve Task Instances for all DAGs
 * and DAG runs.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @param data.logicalDateGte
 * @param data.logicalDateLte
 * @param data.startDateGte
 * @param data.startDateLte
 * @param data.endDateGte
 * @param data.endDateLte
 * @param data.updatedAtGte
 * @param data.updatedAtLte
 * @param data.durationGte
 * @param data.durationLte
 * @param data.state
 * @param data.pool
 * @param data.queue
 * @param data.executor
 * @param data.limit
 * @param data.offset
 * @param data.orderBy
 * @returns TaskInstanceCollectionResponse Successful Response
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
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseTaskInstanceServiceGetTaskInstancesKeyFn({
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
    }),
    queryFn: () =>
      TaskInstanceService.getTaskInstances({
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
      }),
  });
/**
 * Get Task
 * Get simplified representation of a task.
 * @param data The data for the request.
 * @param data.dagId
 * @param data.taskId
 * @returns TaskResponse Successful Response
 * @throws ApiError
 */
export const prefetchUseTaskServiceGetTask = (
  queryClient: QueryClient,
  {
    dagId,
    taskId,
  }: {
    dagId: string;
    taskId: unknown;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseTaskServiceGetTaskKeyFn({ dagId, taskId }),
    queryFn: () => TaskService.getTask({ dagId, taskId }),
  });
/**
 * Get Variable
 * Get a variable entry.
 * @param data The data for the request.
 * @param data.variableKey
 * @returns VariableResponse Successful Response
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
 * Get Variables
 * Get all Variables entries.
 * @param data The data for the request.
 * @param data.limit
 * @param data.offset
 * @param data.orderBy
 * @returns VariableCollectionResponse Successful Response
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
 * Get Version
 * Get version information.
 * @returns VersionInfo Successful Response
 * @throws ApiError
 */
export const prefetchUseVersionServiceGetVersion = (queryClient: QueryClient) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseVersionServiceGetVersionKeyFn(),
    queryFn: () => VersionService.getVersion(),
  });
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
