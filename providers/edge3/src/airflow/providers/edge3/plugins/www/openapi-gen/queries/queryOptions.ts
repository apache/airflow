// generated with @7nohe/openapi-react-query-codegen@3.0.2 

import { queryOptions } from "@tanstack/react-query";
import { health, jobs, logfilePath, worker, type Options } from "../requests/sdk.gen";
import type { HealthData, JobsData, LogfilePathData, WorkerData } from "../requests/types.gen";
import * as Common from "./common";

/**
 * Logfile Path
 *
 * Elaborate the path and filename to expect from task execution.
 */
export const logfilePathOptions = (clientOptions: Options<LogfilePathData, true>, queryKey?: Array<unknown>) => queryOptions({ queryKey: Common.UseLogfilePathKeyFn(clientOptions, queryKey), queryFn: ({ signal }) => logfilePath({ ...clientOptions, signal, throwOnError: true }).then(response => response.data) });
/**
 * Health
 *
 * Report API Health.
 */
export const healthOptions = (clientOptions: Options<HealthData, true> = {}, queryKey?: Array<unknown>) => queryOptions({ queryKey: Common.UseHealthKeyFn(clientOptions, queryKey), queryFn: ({ signal }) => health({ ...clientOptions, signal, throwOnError: true }).then(response => response.data) });
/**
 * Worker
 *
 * Return Edge Workers.
 */
export const workerOptions = (clientOptions: Options<WorkerData, true> = {}, queryKey?: Array<unknown>) => queryOptions({ queryKey: Common.UseWorkerKeyFn(clientOptions, queryKey), queryFn: ({ signal }) => worker({ ...clientOptions, signal, throwOnError: true }).then(response => response.data) });
/**
 * Jobs
 *
 * Return Edge Jobs.
 */
export const jobsOptions = (clientOptions: Options<JobsData, true> = {}, queryKey?: Array<unknown>) => queryOptions({ queryKey: Common.UseJobsKeyFn(clientOptions, queryKey), queryFn: ({ signal }) => jobs({ ...clientOptions, signal, throwOnError: true }).then(response => response.data) });
