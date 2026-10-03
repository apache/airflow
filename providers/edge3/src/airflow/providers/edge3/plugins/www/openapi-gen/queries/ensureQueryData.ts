// generated with @7nohe/openapi-react-query-codegen@3.0.2 

import { type EnsureQueryDataOptions, type QueryClient } from "@tanstack/react-query";
import { health, jobs, logfilePath, worker, type Options } from "../requests/sdk.gen";
import type { HealthData, JobsData, LogfilePathData, WorkerData } from "../requests/types.gen";
import * as Common from "./common";

/**
 * Logfile Path
 *
 * Elaborate the path and filename to expect from task execution.
 */
export const ensureUseLogfilePathData = (queryClient: QueryClient, clientOptions: Options<LogfilePathData, true>, options?: Omit<EnsureQueryDataOptions<Common.LogfilePathDefaultResponse>, "queryKey" | "queryFn">) => queryClient.ensureQueryData({ queryKey: Common.UseLogfilePathKeyFn(clientOptions), queryFn: ({ signal }) => logfilePath({ ...clientOptions, signal, throwOnError: true }).then(response => response.data), ...options });
/**
 * Health
 *
 * Report API Health.
 */
export const ensureUseHealthData = (queryClient: QueryClient, clientOptions: Options<HealthData, true> = {}, options?: Omit<EnsureQueryDataOptions<Common.HealthDefaultResponse>, "queryKey" | "queryFn">) => queryClient.ensureQueryData({ queryKey: Common.UseHealthKeyFn(clientOptions), queryFn: ({ signal }) => health({ ...clientOptions, signal, throwOnError: true }).then(response => response.data), ...options });
/**
 * Worker
 *
 * Return Edge Workers.
 */
export const ensureUseWorkerData = (queryClient: QueryClient, clientOptions: Options<WorkerData, true> = {}, options?: Omit<EnsureQueryDataOptions<Common.WorkerDefaultResponse>, "queryKey" | "queryFn">) => queryClient.ensureQueryData({ queryKey: Common.UseWorkerKeyFn(clientOptions), queryFn: ({ signal }) => worker({ ...clientOptions, signal, throwOnError: true }).then(response => response.data), ...options });
/**
 * Jobs
 *
 * Return Edge Jobs.
 */
export const ensureUseJobsData = (queryClient: QueryClient, clientOptions: Options<JobsData, true> = {}, options?: Omit<EnsureQueryDataOptions<Common.JobsDefaultResponse>, "queryKey" | "queryFn">) => queryClient.ensureQueryData({ queryKey: Common.UseJobsKeyFn(clientOptions), queryFn: ({ signal }) => jobs({ ...clientOptions, signal, throwOnError: true }).then(response => response.data), ...options });
