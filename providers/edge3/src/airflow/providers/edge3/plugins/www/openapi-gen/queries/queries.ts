// generated with @7nohe/openapi-react-query-codegen@3.0.2 

import { useMutation, useQuery, type UseMutationOptions, type UseQueryOptions } from "@tanstack/react-query";
import { AxiosError } from "axios";
import { addWorkerQueue, deleteWorker, exitWorkerMaintenance, fetch_, health, jobs, logfilePath, pushLogs, register, removeWorkerQueue, requestWorkerMaintenance, requestWorkerShutdown, setState, setWorkerConcurrencyLimit, state, updateQueues, updateWorkerMaintenance, worker, type Options } from "../requests/sdk.gen";
import type { AddWorkerQueueData, AddWorkerQueueError, DeleteWorkerData, DeleteWorkerError, ExitWorkerMaintenanceData, ExitWorkerMaintenanceError, FetchData, FetchError, HealthData, JobsData, JobsError, LogfilePathData, LogfilePathError, PushLogsData, PushLogsError, RegisterData, RegisterError, RemoveWorkerQueueData, RemoveWorkerQueueError, RequestWorkerMaintenanceData, RequestWorkerMaintenanceError, RequestWorkerShutdownData, RequestWorkerShutdownError, SetStateData, SetStateError, SetWorkerConcurrencyLimitData, SetWorkerConcurrencyLimitError, StateData, StateError, UpdateQueuesData, UpdateQueuesError, UpdateWorkerMaintenanceData, UpdateWorkerMaintenanceError, WorkerData, WorkerError } from "../requests/types.gen";
import * as Common from "./common";

/**
 * Logfile Path
 *
 * Elaborate the path and filename to expect from task execution.
 */
export const useLogfilePath = <TData = Common.LogfilePathDefaultResponse, TError = AxiosError<LogfilePathError>, TQueryKey extends Array<unknown> = unknown[]>(clientOptions: Options<LogfilePathData, true>, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useQuery<TData, TError>({ queryKey: Common.UseLogfilePathKeyFn(clientOptions, queryKey), queryFn: ({ signal }) => logfilePath({ ...clientOptions, signal, throwOnError: true }).then(response => response.data as TData) as TData, ...options });
/**
 * Health
 *
 * Report API Health.
 */
export const useHealth = <TData = Common.HealthDefaultResponse, TError = AxiosError<unknown>, TQueryKey extends Array<unknown> = unknown[]>(clientOptions: Options<HealthData, true> = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useQuery<TData, TError>({ queryKey: Common.UseHealthKeyFn(clientOptions, queryKey), queryFn: ({ signal }) => health({ ...clientOptions, signal, throwOnError: true }).then(response => response.data as TData) as TData, ...options });
/**
 * Worker
 *
 * Return Edge Workers.
 */
export const useWorker = <TData = Common.WorkerDefaultResponse, TError = AxiosError<WorkerError>, TQueryKey extends Array<unknown> = unknown[]>(clientOptions: Options<WorkerData, true> = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useQuery<TData, TError>({ queryKey: Common.UseWorkerKeyFn(clientOptions, queryKey), queryFn: ({ signal }) => worker({ ...clientOptions, signal, throwOnError: true }).then(response => response.data as TData) as TData, ...options });
/**
 * Jobs
 *
 * Return Edge Jobs.
 */
export const useJobs = <TData = Common.JobsDefaultResponse, TError = AxiosError<JobsError>, TQueryKey extends Array<unknown> = unknown[]>(clientOptions: Options<JobsData, true> = {}, queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useQuery<TData, TError>({ queryKey: Common.UseJobsKeyFn(clientOptions, queryKey), queryFn: ({ signal }) => jobs({ ...clientOptions, signal, throwOnError: true }).then(response => response.data as TData) as TData, ...options });
/**
 * Fetch
 *
 * Fetch a job to execute on the edge worker.
 */
export const useFetch_ = <TData = Common.Fetch_MutationResult, TError = AxiosError<FetchError>, TQueryKey extends Array<unknown> = unknown[], TContext = unknown>(mutationKey?: TQueryKey, options?: Omit<UseMutationOptions<TData, TError, Options<FetchData, true>, TContext>, "mutationKey" | "mutationFn">) => useMutation<TData, TError, Options<FetchData, true>, TContext>({ mutationKey: Common.UseFetch_KeyFn(mutationKey), mutationFn: clientOptions => fetch_({ ...clientOptions, throwOnError: true }) as unknown as Promise<TData>, ...options });
/**
 * State
 *
 * Update the state of a job running on the edge worker.
 */
export const useState = <TData = Common.StateMutationResult, TError = AxiosError<StateError>, TQueryKey extends Array<unknown> = unknown[], TContext = unknown>(mutationKey?: TQueryKey, options?: Omit<UseMutationOptions<TData, TError, Options<StateData, true>, TContext>, "mutationKey" | "mutationFn">) => useMutation<TData, TError, Options<StateData, true>, TContext>({ mutationKey: Common.UseStateKeyFn(mutationKey), mutationFn: clientOptions => state({ ...clientOptions, throwOnError: true }) as unknown as Promise<TData>, ...options });
/**
 * Push Logs
 *
 * Push an incremental log chunk from Edge Worker to central site.
 */
export const usePushLogs = <TData = Common.PushLogsMutationResult, TError = AxiosError<PushLogsError>, TQueryKey extends Array<unknown> = unknown[], TContext = unknown>(mutationKey?: TQueryKey, options?: Omit<UseMutationOptions<TData, TError, Options<PushLogsData, true>, TContext>, "mutationKey" | "mutationFn">) => useMutation<TData, TError, Options<PushLogsData, true>, TContext>({ mutationKey: Common.UsePushLogsKeyFn(mutationKey), mutationFn: clientOptions => pushLogs({ ...clientOptions, throwOnError: true }) as unknown as Promise<TData>, ...options });
/**
 * Set State
 *
 * Set state of worker and returns the current assigned queues.
 */
export const useSetState = <TData = Common.SetStateMutationResult, TError = AxiosError<SetStateError>, TQueryKey extends Array<unknown> = unknown[], TContext = unknown>(mutationKey?: TQueryKey, options?: Omit<UseMutationOptions<TData, TError, Options<SetStateData, true>, TContext>, "mutationKey" | "mutationFn">) => useMutation<TData, TError, Options<SetStateData, true>, TContext>({ mutationKey: Common.UseSetStateKeyFn(mutationKey), mutationFn: clientOptions => setState({ ...clientOptions, throwOnError: true }) as unknown as Promise<TData>, ...options });
/**
 * Register
 *
 * Register a new worker to the backend.
 */
export const useRegister = <TData = Common.RegisterMutationResult, TError = AxiosError<RegisterError>, TQueryKey extends Array<unknown> = unknown[], TContext = unknown>(mutationKey?: TQueryKey, options?: Omit<UseMutationOptions<TData, TError, Options<RegisterData, true>, TContext>, "mutationKey" | "mutationFn">) => useMutation<TData, TError, Options<RegisterData, true>, TContext>({ mutationKey: Common.UseRegisterKeyFn(mutationKey), mutationFn: clientOptions => register({ ...clientOptions, throwOnError: true }) as unknown as Promise<TData>, ...options });
/**
 * Update Queues
 */
export const useUpdateQueues = <TData = Common.UpdateQueuesMutationResult, TError = AxiosError<UpdateQueuesError>, TQueryKey extends Array<unknown> = unknown[], TContext = unknown>(mutationKey?: TQueryKey, options?: Omit<UseMutationOptions<TData, TError, Options<UpdateQueuesData, true>, TContext>, "mutationKey" | "mutationFn">) => useMutation<TData, TError, Options<UpdateQueuesData, true>, TContext>({ mutationKey: Common.UseUpdateQueuesKeyFn(mutationKey), mutationFn: clientOptions => updateQueues({ ...clientOptions, throwOnError: true }) as unknown as Promise<TData>, ...options });
/**
 * Exit Worker Maintenance
 *
 * Exit a worker from maintenance mode.
 */
export const useExitWorkerMaintenance = <TData = Common.ExitWorkerMaintenanceMutationResult, TError = AxiosError<ExitWorkerMaintenanceError>, TQueryKey extends Array<unknown> = unknown[], TContext = unknown>(mutationKey?: TQueryKey, options?: Omit<UseMutationOptions<TData, TError, Options<ExitWorkerMaintenanceData, true>, TContext>, "mutationKey" | "mutationFn">) => useMutation<TData, TError, Options<ExitWorkerMaintenanceData, true>, TContext>({ mutationKey: Common.UseExitWorkerMaintenanceKeyFn(mutationKey), mutationFn: clientOptions => exitWorkerMaintenance({ ...clientOptions, throwOnError: true }) as unknown as Promise<TData>, ...options });
/**
 * Update Worker Maintenance
 *
 * Update maintenance comments for a worker.
 */
export const useUpdateWorkerMaintenance = <TData = Common.UpdateWorkerMaintenanceMutationResult, TError = AxiosError<UpdateWorkerMaintenanceError>, TQueryKey extends Array<unknown> = unknown[], TContext = unknown>(mutationKey?: TQueryKey, options?: Omit<UseMutationOptions<TData, TError, Options<UpdateWorkerMaintenanceData, true>, TContext>, "mutationKey" | "mutationFn">) => useMutation<TData, TError, Options<UpdateWorkerMaintenanceData, true>, TContext>({ mutationKey: Common.UseUpdateWorkerMaintenanceKeyFn(mutationKey), mutationFn: clientOptions => updateWorkerMaintenance({ ...clientOptions, throwOnError: true }) as unknown as Promise<TData>, ...options });
/**
 * Request Worker Maintenance
 *
 * Put a worker into maintenance mode.
 */
export const useRequestWorkerMaintenance = <TData = Common.RequestWorkerMaintenanceMutationResult, TError = AxiosError<RequestWorkerMaintenanceError>, TQueryKey extends Array<unknown> = unknown[], TContext = unknown>(mutationKey?: TQueryKey, options?: Omit<UseMutationOptions<TData, TError, Options<RequestWorkerMaintenanceData, true>, TContext>, "mutationKey" | "mutationFn">) => useMutation<TData, TError, Options<RequestWorkerMaintenanceData, true>, TContext>({ mutationKey: Common.UseRequestWorkerMaintenanceKeyFn(mutationKey), mutationFn: clientOptions => requestWorkerMaintenance({ ...clientOptions, throwOnError: true }) as unknown as Promise<TData>, ...options });
/**
 * Request Worker Shutdown
 *
 * Request shutdown of a worker.
 */
export const useRequestWorkerShutdown = <TData = Common.RequestWorkerShutdownMutationResult, TError = AxiosError<RequestWorkerShutdownError>, TQueryKey extends Array<unknown> = unknown[], TContext = unknown>(mutationKey?: TQueryKey, options?: Omit<UseMutationOptions<TData, TError, Options<RequestWorkerShutdownData, true>, TContext>, "mutationKey" | "mutationFn">) => useMutation<TData, TError, Options<RequestWorkerShutdownData, true>, TContext>({ mutationKey: Common.UseRequestWorkerShutdownKeyFn(mutationKey), mutationFn: clientOptions => requestWorkerShutdown({ ...clientOptions, throwOnError: true }) as unknown as Promise<TData>, ...options });
/**
 * Delete Worker
 *
 * Delete a worker record from the system.
 */
export const useDeleteWorker = <TData = Common.DeleteWorkerMutationResult, TError = AxiosError<DeleteWorkerError>, TQueryKey extends Array<unknown> = unknown[], TContext = unknown>(mutationKey?: TQueryKey, options?: Omit<UseMutationOptions<TData, TError, Options<DeleteWorkerData, true>, TContext>, "mutationKey" | "mutationFn">) => useMutation<TData, TError, Options<DeleteWorkerData, true>, TContext>({ mutationKey: Common.UseDeleteWorkerKeyFn(mutationKey), mutationFn: clientOptions => deleteWorker({ ...clientOptions, throwOnError: true }) as unknown as Promise<TData>, ...options });
/**
 * Remove Worker Queue
 *
 * Remove a queue from a worker.
 */
export const useRemoveWorkerQueue = <TData = Common.RemoveWorkerQueueMutationResult, TError = AxiosError<RemoveWorkerQueueError>, TQueryKey extends Array<unknown> = unknown[], TContext = unknown>(mutationKey?: TQueryKey, options?: Omit<UseMutationOptions<TData, TError, Options<RemoveWorkerQueueData, true>, TContext>, "mutationKey" | "mutationFn">) => useMutation<TData, TError, Options<RemoveWorkerQueueData, true>, TContext>({ mutationKey: Common.UseRemoveWorkerQueueKeyFn(mutationKey), mutationFn: clientOptions => removeWorkerQueue({ ...clientOptions, throwOnError: true }) as unknown as Promise<TData>, ...options });
/**
 * Add Worker Queue
 *
 * Add a queue to a worker.
 */
export const useAddWorkerQueue = <TData = Common.AddWorkerQueueMutationResult, TError = AxiosError<AddWorkerQueueError>, TQueryKey extends Array<unknown> = unknown[], TContext = unknown>(mutationKey?: TQueryKey, options?: Omit<UseMutationOptions<TData, TError, Options<AddWorkerQueueData, true>, TContext>, "mutationKey" | "mutationFn">) => useMutation<TData, TError, Options<AddWorkerQueueData, true>, TContext>({ mutationKey: Common.UseAddWorkerQueueKeyFn(mutationKey), mutationFn: clientOptions => addWorkerQueue({ ...clientOptions, throwOnError: true }) as unknown as Promise<TData>, ...options });
/**
 * Set Worker Concurrency Limit
 *
 * Set the concurrency limit for an edge worker.
 */
export const useSetWorkerConcurrencyLimit = <TData = Common.SetWorkerConcurrencyLimitMutationResult, TError = AxiosError<SetWorkerConcurrencyLimitError>, TQueryKey extends Array<unknown> = unknown[], TContext = unknown>(mutationKey?: TQueryKey, options?: Omit<UseMutationOptions<TData, TError, Options<SetWorkerConcurrencyLimitData, true>, TContext>, "mutationKey" | "mutationFn">) => useMutation<TData, TError, Options<SetWorkerConcurrencyLimitData, true>, TContext>({ mutationKey: Common.UseSetWorkerConcurrencyLimitKeyFn(mutationKey), mutationFn: clientOptions => setWorkerConcurrencyLimit({ ...clientOptions, throwOnError: true }) as unknown as Promise<TData>, ...options });
