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
  } = {}
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseConnectionServiceGetConnectionsKeyFn({
      limit,
      offset,
      orderBy,
    }),
    queryFn: () => ConnectionService.getConnections({ limit, offset, orderBy }),
  });
export const prefetchUseConnectionServiceGetConnection = (
  queryClient: QueryClient,
  {
    connectionId,
  }: {
    connectionId: string;
  }
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseConnectionServiceGetConnectionKeyFn({ connectionId }),
    queryFn: () => ConnectionService.getConnection({ connectionId }),
  });
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
  } = {}
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
export const prefetchUseDagServiceGetDag = (
  queryClient: QueryClient,
  {
    dagId,
    fields,
  }: {
    dagId: string;
    fields?: string[];
  }
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagServiceGetDagKeyFn({ dagId, fields }),
    queryFn: () => DagService.getDag({ dagId, fields }),
  });
export const prefetchUseDagServiceGetDagDetails = (
  queryClient: QueryClient,
  {
    dagId,
    fields,
  }: {
    dagId: string;
    fields?: string[];
  }
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagServiceGetDagDetailsKeyFn({ dagId, fields }),
    queryFn: () => DagService.getDagDetails({ dagId, fields }),
  });
export const prefetchUseDagServiceGetTasks = (
  queryClient: QueryClient,
  {
    dagId,
    orderBy,
  }: {
    dagId: string;
    orderBy?: string;
  }
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagServiceGetTasksKeyFn({ dagId, orderBy }),
    queryFn: () => DagService.getTasks({ dagId, orderBy }),
  });
export const prefetchUseDagServiceGetTask = (
  queryClient: QueryClient,
  {
    dagId,
    taskId,
  }: {
    dagId: string;
    taskId: string;
  }
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagServiceGetTaskKeyFn({ dagId, taskId }),
    queryFn: () => DagService.getTask({ dagId, taskId }),
  });
export const prefetchUseDagServiceGetDagSource = (
  queryClient: QueryClient,
  {
    fileToken,
  }: {
    fileToken: string;
  }
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagServiceGetDagSourceKeyFn({ fileToken }),
    queryFn: () => DagService.getDagSource({ fileToken }),
  });
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
  }
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
  }
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
  }
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
  }
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
  }
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
  }
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
  }
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
  }
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
  }
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
  }
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseTaskInstanceServiceGetMappedTaskInstanceTryDetailsKeyFn(
      { dagId, dagRunId, mapIndex, taskId, taskTryNumber }
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
  }
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
  }
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
  }
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
  }
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagRunServiceGetDagRunKeyFn({
      dagId,
      dagRunId,
      fields,
    }),
    queryFn: () => DagRunService.getDagRun({ dagId, dagRunId, fields }),
  });
export const prefetchUseDagRunServiceGetUpstreamDatasetEvents = (
  queryClient: QueryClient,
  {
    dagId,
    dagRunId,
  }: {
    dagId: string;
    dagRunId: string;
  }
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagRunServiceGetUpstreamDatasetEventsKeyFn({
      dagId,
      dagRunId,
    }),
    queryFn: () => DagRunService.getUpstreamDatasetEvents({ dagId, dagRunId }),
  });
export const prefetchUseDatasetServiceGetUpstreamDatasetEvents = (
  queryClient: QueryClient,
  {
    dagId,
    dagRunId,
  }: {
    dagId: string;
    dagRunId: string;
  }
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDatasetServiceGetUpstreamDatasetEventsKeyFn({
      dagId,
      dagRunId,
    }),
    queryFn: () => DatasetService.getUpstreamDatasetEvents({ dagId, dagRunId }),
  });
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
  }
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
export const prefetchUseDatasetServiceGetDagDatasetQueuedEvents = (
  queryClient: QueryClient,
  {
    before,
    dagId,
  }: {
    before?: string;
    dagId: string;
  }
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDatasetServiceGetDagDatasetQueuedEventsKeyFn({
      before,
      dagId,
    }),
    queryFn: () => DatasetService.getDagDatasetQueuedEvents({ before, dagId }),
  });
export const prefetchUseDatasetServiceGetDatasetQueuedEvents = (
  queryClient: QueryClient,
  {
    before,
    uri,
  }: {
    before?: string;
    uri: string;
  }
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDatasetServiceGetDatasetQueuedEventsKeyFn({
      before,
      uri,
    }),
    queryFn: () => DatasetService.getDatasetQueuedEvents({ before, uri }),
  });
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
  } = {}
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
export const prefetchUseDatasetServiceGetDataset = (
  queryClient: QueryClient,
  {
    uri,
  }: {
    uri: string;
  }
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDatasetServiceGetDatasetKeyFn({ uri }),
    queryFn: () => DatasetService.getDataset({ uri }),
  });
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
  } = {}
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
  } = {}
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
export const prefetchUseEventLogServiceGetEventLog = (
  queryClient: QueryClient,
  {
    eventLogId,
  }: {
    eventLogId: number;
  }
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseEventLogServiceGetEventLogKeyFn({ eventLogId }),
    queryFn: () => EventLogService.getEventLog({ eventLogId }),
  });
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
  } = {}
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
export const prefetchUseImportErrorServiceGetImportError = (
  queryClient: QueryClient,
  {
    importErrorId,
  }: {
    importErrorId: number;
  }
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseImportErrorServiceGetImportErrorKeyFn({
      importErrorId,
    }),
    queryFn: () => ImportErrorService.getImportError({ importErrorId }),
  });
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
  } = {}
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UsePoolServiceGetPoolsKeyFn({ limit, offset, orderBy }),
    queryFn: () => PoolService.getPools({ limit, offset, orderBy }),
  });
export const prefetchUsePoolServiceGetPool = (
  queryClient: QueryClient,
  {
    poolName,
  }: {
    poolName: string;
  }
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UsePoolServiceGetPoolKeyFn({ poolName }),
    queryFn: () => PoolService.getPool({ poolName }),
  });
export const prefetchUseProviderServiceGetProviders = (
  queryClient: QueryClient
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseProviderServiceGetProvidersKeyFn(),
    queryFn: () => ProviderService.getProviders(),
  });
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
  } = {}
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseVariableServiceGetVariablesKeyFn({
      limit,
      offset,
      orderBy,
    }),
    queryFn: () => VariableService.getVariables({ limit, offset, orderBy }),
  });
export const prefetchUseVariableServiceGetVariable = (
  queryClient: QueryClient,
  {
    variableKey,
  }: {
    variableKey: string;
  }
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseVariableServiceGetVariableKeyFn({ variableKey }),
    queryFn: () => VariableService.getVariable({ variableKey }),
  });
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
  }
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
  }
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
export const prefetchUseDagStatsServiceGetDagStats = (
  queryClient: QueryClient,
  {
    dagIds,
  }: {
    dagIds: string;
  }
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagStatsServiceGetDagStatsKeyFn({ dagIds }),
    queryFn: () => DagStatsService.getDagStats({ dagIds }),
  });
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
  } = {}
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
export const prefetchUseConfigServiceGetConfig = (
  queryClient: QueryClient,
  {
    section,
  }: {
    section?: string;
  } = {}
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseConfigServiceGetConfigKeyFn({ section }),
    queryFn: () => ConfigService.getConfig({ section }),
  });
export const prefetchUseConfigServiceGetValue = (
  queryClient: QueryClient,
  {
    option,
    section,
  }: {
    option: string;
    section: string;
  }
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseConfigServiceGetValueKeyFn({ option, section }),
    queryFn: () => ConfigService.getValue({ option, section }),
  });
export const prefetchUseMonitoringServiceGetHealth = (
  queryClient: QueryClient
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseMonitoringServiceGetHealthKeyFn(),
    queryFn: () => MonitoringService.getHealth(),
  });
export const prefetchUseMonitoringServiceGetVersion = (
  queryClient: QueryClient
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseMonitoringServiceGetVersionKeyFn(),
    queryFn: () => MonitoringService.getVersion(),
  });
export const prefetchUsePluginServiceGetPlugins = (
  queryClient: QueryClient,
  {
    limit,
    offset,
  }: {
    limit?: number;
    offset?: number;
  } = {}
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UsePluginServiceGetPluginsKeyFn({ limit, offset }),
    queryFn: () => PluginService.getPlugins({ limit, offset }),
  });
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
  } = {}
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseRoleServiceGetRolesKeyFn({ limit, offset, orderBy }),
    queryFn: () => RoleService.getRoles({ limit, offset, orderBy }),
  });
export const prefetchUseRoleServiceGetRole = (
  queryClient: QueryClient,
  {
    roleName,
  }: {
    roleName: string;
  }
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseRoleServiceGetRoleKeyFn({ roleName }),
    queryFn: () => RoleService.getRole({ roleName }),
  });
export const prefetchUsePermissionServiceGetPermissions = (
  queryClient: QueryClient,
  {
    limit,
    offset,
  }: {
    limit?: number;
    offset?: number;
  } = {}
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UsePermissionServiceGetPermissionsKeyFn({ limit, offset }),
    queryFn: () => PermissionService.getPermissions({ limit, offset }),
  });
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
  } = {}
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseUserServiceGetUsersKeyFn({ limit, offset, orderBy }),
    queryFn: () => UserService.getUsers({ limit, offset, orderBy }),
  });
export const prefetchUseUserServiceGetUser = (
  queryClient: QueryClient,
  {
    username,
  }: {
    username: string;
  }
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseUserServiceGetUserKeyFn({ username }),
    queryFn: () => UserService.getUser({ username }),
  });
