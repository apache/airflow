// generated with @7nohe/openapi-react-query-codegen@3.0.2 

import { useSuspenseQuery, type UseSuspenseQueryOptions } from "@tanstack/react-query";
import { AxiosError } from "axios";
import { health, jobs, logfilePath, worker, type Options } from "../requests/sdk.gen";
import type { HealthData, JobsData, JobsError, LogfilePathData, LogfilePathError, WorkerData, WorkerError } from "../requests/types.gen";
import * as Common from "./common";

/**
 * Logfile Path
 *
 * Elaborate the path and filename to expect from task execution.
 */
export const useLogfilePathSuspense = <TData = NonNullable<Common.LogfilePathDefaultResponse>, TError = AxiosError<LogfilePathError>, TQueryKey extends Array<unknown> = unknown[]>(clientOptions: Options<LogfilePathData, true>, queryKey?: TQueryKey, options?: Omit<UseSuspenseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseLogfilePathKeyFn(clientOptions, queryKey), queryFn: ({ signal }) => logfilePath({ ...clientOptions, signal, throwOnError: true }).then(response => response.data as TData) as TData, ...options });
/**
 * Health
 *
 * Report API Health.
 */
export const useHealthSuspense = <TData = NonNullable<Common.HealthDefaultResponse>, TError = AxiosError<unknown>, TQueryKey extends Array<unknown> = unknown[]>(clientOptions: Options<HealthData, true> = {}, queryKey?: TQueryKey, options?: Omit<UseSuspenseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseHealthKeyFn(clientOptions, queryKey), queryFn: ({ signal }) => health({ ...clientOptions, signal, throwOnError: true }).then(response => response.data as TData) as TData, ...options });
/**
 * Worker
 *
 * Return Edge Workers.
 */
export const useWorkerSuspense = <TData = NonNullable<Common.WorkerDefaultResponse>, TError = AxiosError<WorkerError>, TQueryKey extends Array<unknown> = unknown[]>(clientOptions: Options<WorkerData, true> = {}, queryKey?: TQueryKey, options?: Omit<UseSuspenseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseWorkerKeyFn(clientOptions, queryKey), queryFn: ({ signal }) => worker({ ...clientOptions, signal, throwOnError: true }).then(response => response.data as TData) as TData, ...options });
/**
 * Jobs
 *
 * Return Edge Jobs.
 */
export const useJobsSuspense = <TData = NonNullable<Common.JobsDefaultResponse>, TError = AxiosError<JobsError>, TQueryKey extends Array<unknown> = unknown[]>(clientOptions: Options<JobsData, true> = {}, queryKey?: TQueryKey, options?: Omit<UseSuspenseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useSuspenseQuery<TData, TError>({ queryKey: Common.UseJobsKeyFn(clientOptions, queryKey), queryFn: ({ signal }) => jobs({ ...clientOptions, signal, throwOnError: true }).then(response => response.data as TData) as TData, ...options });
