// generated with @7nohe/openapi-react-query-codegen@1.6.2 

import { UseQueryOptions, useSuspenseQuery } from "@tanstack/react-query";
import { LogsService, MonitorService, UiService } from "../requests/services.gen";
import { EdgeWorkerState } from "../requests/types.gen";
import * as Common from "./common";
export const useLogsServiceLogfilePathSuspense = <TData = Common.LogsServiceLogfilePathDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ authorization, dagId, mapIndex, runId, taskId, tryNumber }: {
  authorization: string;
  dagId: string;
  mapIndex: number;
  runId: string;
  taskId: string;
  tryNumber: number;
}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseLogsServiceLogfilePathKeyFn({ authorization, dagId, mapIndex, runId, taskId, tryNumber }, queryKey), queryFn: () => LogsService.logfilePath({ authorization, dagId, mapIndex, runId, taskId, tryNumber }) as TData, ...options });
export const useMonitorServiceHealthSuspense = <TData = Common.MonitorServiceHealthDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>(queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseMonitorServiceHealthKeyFn(queryKey), queryFn: () => MonitorService.health() as TData, ...options });
export const useUiServiceWorkerSuspense = <TData = Common.UiServiceWorkerDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>({ queueNamePattern, state, workerNamePattern }: {
  queueNamePattern?: string;
  state?: EdgeWorkerState[];
  workerNamePattern?: string;
} = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseUiServiceWorkerKeyFn({ queueNamePattern, state, workerNamePattern }, queryKey), queryFn: () => UiService.worker({ queueNamePattern, state, workerNamePattern }) as TData, ...options });
export const useUiServiceJobsSuspense = <TData = Common.UiServiceJobsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>(queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseUiServiceJobsKeyFn(queryKey), queryFn: () => UiService.jobs() as TData, ...options });
