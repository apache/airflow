// generated with @7nohe/openapi-react-query-codegen@1.6.2 

import { UseQueryResult } from "@tanstack/react-query";
import { JobsService, LogsService, MonitorService, UiService, WorkerService } from "../requests/services.gen";
import { EdgeWorkerState } from "../requests/types.gen";
export type LogsServiceLogfilePathDefaultResponse = Awaited<ReturnType<typeof LogsService.logfilePath>>;
export type LogsServiceLogfilePathQueryResult<TData = LogsServiceLogfilePathDefaultResponse, TError = unknown> = UseQueryResult<TData, TError>;
export const useLogsServiceLogfilePathKey = "LogsServiceLogfilePath";
export const UseLogsServiceLogfilePathKeyFn = ({ authorization, dagId, mapIndex, runId, taskId, tryNumber }: {
  authorization: string;
  dagId: string;
  mapIndex: number;
  runId: string;
  taskId: string;
  tryNumber: number;
}, queryKey?: Array<unknown>) => [useLogsServiceLogfilePathKey, ...(queryKey ?? [{ authorization, dagId, mapIndex, runId, taskId, tryNumber }])];
export type MonitorServiceHealthDefaultResponse = Awaited<ReturnType<typeof MonitorService.health>>;
export type MonitorServiceHealthQueryResult<TData = MonitorServiceHealthDefaultResponse, TError = unknown> = UseQueryResult<TData, TError>;
export const useMonitorServiceHealthKey = "MonitorServiceHealth";
export const UseMonitorServiceHealthKeyFn = (queryKey?: Array<unknown>) => [useMonitorServiceHealthKey, ...(queryKey ?? [])];
export type UiServiceWorkerDefaultResponse = Awaited<ReturnType<typeof UiService.worker>>;
export type UiServiceWorkerQueryResult<TData = UiServiceWorkerDefaultResponse, TError = unknown> = UseQueryResult<TData, TError>;
export const useUiServiceWorkerKey = "UiServiceWorker";
export const UseUiServiceWorkerKeyFn = ({ queueNamePattern, state, workerNamePattern }: {
  queueNamePattern?: string;
  state?: EdgeWorkerState[];
  workerNamePattern?: string;
} = {}, queryKey?: Array<unknown>) => [useUiServiceWorkerKey, ...(queryKey ?? [{ queueNamePattern, state, workerNamePattern }])];
export type UiServiceJobsDefaultResponse = Awaited<ReturnType<typeof UiService.jobs>>;
export type UiServiceJobsQueryResult<TData = UiServiceJobsDefaultResponse, TError = unknown> = UseQueryResult<TData, TError>;
export const useUiServiceJobsKey = "UiServiceJobs";
export const UseUiServiceJobsKeyFn = (queryKey?: Array<unknown>) => [useUiServiceJobsKey, ...(queryKey ?? [])];
export type JobsServiceFetchMutationResult = Awaited<ReturnType<typeof JobsService.fetch>>;
export type LogsServicePushLogsMutationResult = Awaited<ReturnType<typeof LogsService.pushLogs>>;
export type WorkerServiceRegisterMutationResult = Awaited<ReturnType<typeof WorkerService.register>>;
export type UiServiceRequestWorkerMaintenanceMutationResult = Awaited<ReturnType<typeof UiService.requestWorkerMaintenance>>;
export type UiServiceRequestWorkerShutdownMutationResult = Awaited<ReturnType<typeof UiService.requestWorkerShutdown>>;
export type UiServiceAddWorkerQueueMutationResult = Awaited<ReturnType<typeof UiService.addWorkerQueue>>;
export type JobsServiceStateMutationResult = Awaited<ReturnType<typeof JobsService.state>>;
export type WorkerServiceSetStateMutationResult = Awaited<ReturnType<typeof WorkerService.setState>>;
export type WorkerServiceUpdateQueuesMutationResult = Awaited<ReturnType<typeof WorkerService.updateQueues>>;
export type UiServiceUpdateWorkerMaintenanceMutationResult = Awaited<ReturnType<typeof UiService.updateWorkerMaintenance>>;
export type UiServiceExitWorkerMaintenanceMutationResult = Awaited<ReturnType<typeof UiService.exitWorkerMaintenance>>;
export type UiServiceDeleteWorkerMutationResult = Awaited<ReturnType<typeof UiService.deleteWorker>>;
export type UiServiceRemoveWorkerQueueMutationResult = Awaited<ReturnType<typeof UiService.removeWorkerQueue>>;
