// generated with @7nohe/openapi-react-query-codegen@3.0.2 

import { type FetchQueryOptions, type QueryClient } from "@tanstack/react-query";
import { health, jobs, logfilePath, worker, type Options } from "../requests/sdk.gen";
import type { HealthData, JobsData, LogfilePathData, WorkerData } from "../requests/types.gen";
import * as Common from "./common";

/**
 * Logfile Path
 *
 * Elaborate the path and filename to expect from task execution.
 */
export const prefetchUseLogfilePath = (queryClient: QueryClient, clientOptions: Options<LogfilePathData, true>, options?: Omit<FetchQueryOptions<Common.LogfilePathDefaultResponse>, "queryKey" | "queryFn">) => queryClient.prefetchQuery({ queryKey: Common.UseLogfilePathKeyFn(clientOptions), queryFn: ({ signal }) => logfilePath({ ...clientOptions, signal, throwOnError: true }).then(response => response.data), ...options });
/**
 * Health
 *
 * Report API Health.
 */
export const prefetchUseHealth = (queryClient: QueryClient, clientOptions: Options<HealthData, true> = {}, options?: Omit<FetchQueryOptions<Common.HealthDefaultResponse>, "queryKey" | "queryFn">) => queryClient.prefetchQuery({ queryKey: Common.UseHealthKeyFn(clientOptions), queryFn: ({ signal }) => health({ ...clientOptions, signal, throwOnError: true }).then(response => response.data), ...options });
/**
 * Worker
 *
 * Return Edge Workers.
 */
export const prefetchUseWorker = (queryClient: QueryClient, clientOptions: Options<WorkerData, true> = {}, options?: Omit<FetchQueryOptions<Common.WorkerDefaultResponse>, "queryKey" | "queryFn">) => queryClient.prefetchQuery({ queryKey: Common.UseWorkerKeyFn(clientOptions), queryFn: ({ signal }) => worker({ ...clientOptions, signal, throwOnError: true }).then(response => response.data), ...options });
/**
 * Jobs
 *
 * Return Edge Jobs.
 */
export const prefetchUseJobs = (queryClient: QueryClient, clientOptions: Options<JobsData, true> = {}, options?: Omit<FetchQueryOptions<Common.JobsDefaultResponse>, "queryKey" | "queryFn">) => queryClient.prefetchQuery({ queryKey: Common.UseJobsKeyFn(clientOptions), queryFn: ({ signal }) => jobs({ ...clientOptions, signal, throwOnError: true }).then(response => response.data), ...options });
