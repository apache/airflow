// generated with @7nohe/openapi-react-query-codegen@1.6.2 

import { UseMutationOptions, UseQueryOptions, useMutation, useQuery } from "@tanstack/react-query";
import { JobsService, LogsService, MonitorService, UiService, WorkerService } from "../requests/services.gen";
import { MaintenanceRequest, PushLogsBody, TaskInstanceState, WorkerQueueUpdateBody, WorkerQueuesBody, WorkerStateBody } from "../requests/types.gen";
import * as Common from "./common";
export const useLogsServiceLogfilePath = <TData = Common.LogsServiceLogfilePathDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ authorization, dagId, mapIndex, runId, taskId, tryNumber }: {
  authorization: string;
  dagId: string;
  mapIndex: number;
  runId: string;
  taskId: string;
  tryNumber: number;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useQuery<TData, TError>({ queryKey: Common.UseLogsServiceLogfilePathKeyFn({ authorization, dagId, mapIndex, runId, taskId, tryNumber }, queryKey), queryFn: () => LogsService.logfilePath({ authorization, dagId, mapIndex, runId, taskId, tryNumber }) as TData, ...options });
export const useMonitorServiceHealth = <TData = Common.MonitorServiceHealthDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>(queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useQuery<TData, TError>({ queryKey: Common.UseMonitorServiceHealthKeyFn(queryKey), queryFn: () => MonitorService.health() as TData, ...options });
export const useUiServiceWorker = <TData = Common.UiServiceWorkerDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>(queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useQuery<TData, TError>({ queryKey: Common.UseUiServiceWorkerKeyFn(queryKey), queryFn: () => UiService.worker() as TData, ...options });
export const useUiServiceJobs = <TData = Common.UiServiceJobsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>(queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useQuery<TData, TError>({ queryKey: Common.UseUiServiceJobsKeyFn(queryKey), queryFn: () => UiService.jobs() as TData, ...options });
export const useJobsServiceFetch = <TData = Common.JobsServiceFetchMutationResult, TError = unknown, TContext = unknown>(options?: Omit<UseMutationOptions<TData, TError, {
  authorization: string;
  requestBody: WorkerQueuesBody;
  workerName: string;
}, TContext>, "mutationFn">) => useMutation<TData, TError, {
  authorization: string;
  requestBody: WorkerQueuesBody;
  workerName: string;
}, TContext>({ mutationFn: ({ authorization, requestBody, workerName }) => JobsService.fetch({ authorization, requestBody, workerName }) as unknown as Promise<TData>, ...options });
export const useLogsServicePushLogs = <TData = Common.LogsServicePushLogsMutationResult, TError = unknown, TContext = unknown>(options?: Omit<UseMutationOptions<TData, TError, {
  authorization: string;
  dagId: string;
  mapIndex: number;
  requestBody: PushLogsBody;
  runId: string;
  taskId: string;
  tryNumber: number;
}, TContext>, "mutationFn">) => useMutation<TData, TError, {
  authorization: string;
  dagId: string;
  mapIndex: number;
  requestBody: PushLogsBody;
  runId: string;
  taskId: string;
  tryNumber: number;
}, TContext>({ mutationFn: ({ authorization, dagId, mapIndex, requestBody, runId, taskId, tryNumber }) => LogsService.pushLogs({ authorization, dagId, mapIndex, requestBody, runId, taskId, tryNumber }) as unknown as Promise<TData>, ...options });
export const useWorkerServiceRegister = <TData = Common.WorkerServiceRegisterMutationResult, TError = unknown, TContext = unknown>(options?: Omit<UseMutationOptions<TData, TError, {
  authorization: string;
  requestBody: WorkerStateBody;
  workerName: string;
}, TContext>, "mutationFn">) => useMutation<TData, TError, {
  authorization: string;
  requestBody: WorkerStateBody;
  workerName: string;
}, TContext>({ mutationFn: ({ authorization, requestBody, workerName }) => WorkerService.register({ authorization, requestBody, workerName }) as unknown as Promise<TData>, ...options });
export const useUiServiceRequestWorkerMaintenance = <TData = Common.UiServiceRequestWorkerMaintenanceMutationResult, TError = unknown, TContext = unknown>(options?: Omit<UseMutationOptions<TData, TError, {
  requestBody: MaintenanceRequest;
  workerName: string;
}, TContext>, "mutationFn">) => useMutation<TData, TError, {
  requestBody: MaintenanceRequest;
  workerName: string;
}, TContext>({ mutationFn: ({ requestBody, workerName }) => UiService.requestWorkerMaintenance({ requestBody, workerName }) as unknown as Promise<TData>, ...options });
export const useUiServiceRequestWorkerShutdown = <TData = Common.UiServiceRequestWorkerShutdownMutationResult, TError = unknown, TContext = unknown>(options?: Omit<UseMutationOptions<TData, TError, {
  workerName: string;
}, TContext>, "mutationFn">) => useMutation<TData, TError, {
  workerName: string;
}, TContext>({ mutationFn: ({ workerName }) => UiService.requestWorkerShutdown({ workerName }) as unknown as Promise<TData>, ...options });
export const useJobsServiceState = <TData = Common.JobsServiceStateMutationResult, TError = unknown, TContext = unknown>(options?: Omit<UseMutationOptions<TData, TError, {
  authorization: string;
  dagId: string;
  mapIndex: number;
  runId: string;
  state: TaskInstanceState;
  taskId: string;
  tryNumber: number;
}, TContext>, "mutationFn">) => useMutation<TData, TError, {
  authorization: string;
  dagId: string;
  mapIndex: number;
  runId: string;
  state: TaskInstanceState;
  taskId: string;
  tryNumber: number;
}, TContext>({ mutationFn: ({ authorization, dagId, mapIndex, runId, state, taskId, tryNumber }) => JobsService.state({ authorization, dagId, mapIndex, runId, state, taskId, tryNumber }) as unknown as Promise<TData>, ...options });
export const useWorkerServiceSetState = <TData = Common.WorkerServiceSetStateMutationResult, TError = unknown, TContext = unknown>(options?: Omit<UseMutationOptions<TData, TError, {
  authorization: string;
  requestBody: WorkerStateBody;
  workerName: string;
}, TContext>, "mutationFn">) => useMutation<TData, TError, {
  authorization: string;
  requestBody: WorkerStateBody;
  workerName: string;
}, TContext>({ mutationFn: ({ authorization, requestBody, workerName }) => WorkerService.setState({ authorization, requestBody, workerName }) as unknown as Promise<TData>, ...options });
export const useWorkerServiceUpdateQueues = <TData = Common.WorkerServiceUpdateQueuesMutationResult, TError = unknown, TContext = unknown>(options?: Omit<UseMutationOptions<TData, TError, {
  authorization: string;
  requestBody: WorkerQueueUpdateBody;
  workerName: string;
}, TContext>, "mutationFn">) => useMutation<TData, TError, {
  authorization: string;
  requestBody: WorkerQueueUpdateBody;
  workerName: string;
}, TContext>({ mutationFn: ({ authorization, requestBody, workerName }) => WorkerService.updateQueues({ authorization, requestBody, workerName }) as unknown as Promise<TData>, ...options });
export const useUiServiceExitWorkerMaintenance = <TData = Common.UiServiceExitWorkerMaintenanceMutationResult, TError = unknown, TContext = unknown>(options?: Omit<UseMutationOptions<TData, TError, {
  workerName: string;
}, TContext>, "mutationFn">) => useMutation<TData, TError, {
  workerName: string;
}, TContext>({ mutationFn: ({ workerName }) => UiService.exitWorkerMaintenance({ workerName }) as unknown as Promise<TData>, ...options });
export const useUiServiceDeleteWorker = <TData = Common.UiServiceDeleteWorkerMutationResult, TError = unknown, TContext = unknown>(options?: Omit<UseMutationOptions<TData, TError, {
  workerName: string;
}, TContext>, "mutationFn">) => useMutation<TData, TError, {
  workerName: string;
}, TContext>({ mutationFn: ({ workerName }) => UiService.deleteWorker({ workerName }) as unknown as Promise<TData>, ...options });
