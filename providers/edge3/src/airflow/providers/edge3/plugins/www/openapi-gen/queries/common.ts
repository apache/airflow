// generated with @7nohe/openapi-react-query-codegen@3.0.2 

import { type UseQueryResult } from "@tanstack/react-query";
import { addWorkerQueue, deleteWorker, exitWorkerMaintenance, fetch_, health, jobs, logfilePath, pushLogs, register, removeWorkerQueue, requestWorkerMaintenance, requestWorkerShutdown, setState, setWorkerConcurrencyLimit, state, updateQueues, updateWorkerMaintenance, worker, type Options } from "../requests/sdk.gen";
import type { HealthData, JobsData, LogfilePathData, WorkerData } from "../requests/types.gen";

export type LogfilePathDefaultResponse = Awaited<ReturnType<typeof logfilePath>>["data"];
export type LogfilePathQueryResult<TData = LogfilePathDefaultResponse, TError = unknown> = UseQueryResult<TData, TError>;

export const useLogfilePathKey = "LogfilePath";
export const UseLogfilePathKeyFn = (clientOptions: Options<LogfilePathData, true>, queryKey?: Array<unknown>) => [useLogfilePathKey, ...(queryKey ?? [clientOptions])];

export type HealthDefaultResponse = Awaited<ReturnType<typeof health>>["data"];
export type HealthQueryResult<TData = HealthDefaultResponse, TError = unknown> = UseQueryResult<TData, TError>;

export const useHealthKey = "Health";
export const UseHealthKeyFn = (clientOptions: Options<HealthData, true> = {}, queryKey?: Array<unknown>) => [useHealthKey, ...(queryKey ?? [clientOptions])];

export type WorkerDefaultResponse = Awaited<ReturnType<typeof worker>>["data"];
export type WorkerQueryResult<TData = WorkerDefaultResponse, TError = unknown> = UseQueryResult<TData, TError>;

export const useWorkerKey = "Worker";
export const UseWorkerKeyFn = (clientOptions: Options<WorkerData, true> = {}, queryKey?: Array<unknown>) => [useWorkerKey, ...(queryKey ?? [clientOptions])];

export type JobsDefaultResponse = Awaited<ReturnType<typeof jobs>>["data"];
export type JobsQueryResult<TData = JobsDefaultResponse, TError = unknown> = UseQueryResult<TData, TError>;

export const useJobsKey = "Jobs";
export const UseJobsKeyFn = (clientOptions: Options<JobsData, true> = {}, queryKey?: Array<unknown>) => [useJobsKey, ...(queryKey ?? [clientOptions])];

export type Fetch_MutationResult = Awaited<ReturnType<typeof fetch_>>;

export const useFetch_Key = "Fetch_";
export const UseFetch_KeyFn = (mutationKey?: Array<unknown>) => [useFetch_Key, ...(mutationKey ?? [])];

export type StateMutationResult = Awaited<ReturnType<typeof state>>;

export const useStateKey = "State";
export const UseStateKeyFn = (mutationKey?: Array<unknown>) => [useStateKey, ...(mutationKey ?? [])];

export type PushLogsMutationResult = Awaited<ReturnType<typeof pushLogs>>;

export const usePushLogsKey = "PushLogs";
export const UsePushLogsKeyFn = (mutationKey?: Array<unknown>) => [usePushLogsKey, ...(mutationKey ?? [])];

export type SetStateMutationResult = Awaited<ReturnType<typeof setState>>;

export const useSetStateKey = "SetState";
export const UseSetStateKeyFn = (mutationKey?: Array<unknown>) => [useSetStateKey, ...(mutationKey ?? [])];

export type RegisterMutationResult = Awaited<ReturnType<typeof register>>;

export const useRegisterKey = "Register";
export const UseRegisterKeyFn = (mutationKey?: Array<unknown>) => [useRegisterKey, ...(mutationKey ?? [])];

export type UpdateQueuesMutationResult = Awaited<ReturnType<typeof updateQueues>>;

export const useUpdateQueuesKey = "UpdateQueues";
export const UseUpdateQueuesKeyFn = (mutationKey?: Array<unknown>) => [useUpdateQueuesKey, ...(mutationKey ?? [])];

export type ExitWorkerMaintenanceMutationResult = Awaited<ReturnType<typeof exitWorkerMaintenance>>;

export const useExitWorkerMaintenanceKey = "ExitWorkerMaintenance";
export const UseExitWorkerMaintenanceKeyFn = (mutationKey?: Array<unknown>) => [useExitWorkerMaintenanceKey, ...(mutationKey ?? [])];

export type UpdateWorkerMaintenanceMutationResult = Awaited<ReturnType<typeof updateWorkerMaintenance>>;

export const useUpdateWorkerMaintenanceKey = "UpdateWorkerMaintenance";
export const UseUpdateWorkerMaintenanceKeyFn = (mutationKey?: Array<unknown>) => [useUpdateWorkerMaintenanceKey, ...(mutationKey ?? [])];

export type RequestWorkerMaintenanceMutationResult = Awaited<ReturnType<typeof requestWorkerMaintenance>>;

export const useRequestWorkerMaintenanceKey = "RequestWorkerMaintenance";
export const UseRequestWorkerMaintenanceKeyFn = (mutationKey?: Array<unknown>) => [useRequestWorkerMaintenanceKey, ...(mutationKey ?? [])];

export type RequestWorkerShutdownMutationResult = Awaited<ReturnType<typeof requestWorkerShutdown>>;

export const useRequestWorkerShutdownKey = "RequestWorkerShutdown";
export const UseRequestWorkerShutdownKeyFn = (mutationKey?: Array<unknown>) => [useRequestWorkerShutdownKey, ...(mutationKey ?? [])];

export type DeleteWorkerMutationResult = Awaited<ReturnType<typeof deleteWorker>>;

export const useDeleteWorkerKey = "DeleteWorker";
export const UseDeleteWorkerKeyFn = (mutationKey?: Array<unknown>) => [useDeleteWorkerKey, ...(mutationKey ?? [])];

export type RemoveWorkerQueueMutationResult = Awaited<ReturnType<typeof removeWorkerQueue>>;

export const useRemoveWorkerQueueKey = "RemoveWorkerQueue";
export const UseRemoveWorkerQueueKeyFn = (mutationKey?: Array<unknown>) => [useRemoveWorkerQueueKey, ...(mutationKey ?? [])];

export type AddWorkerQueueMutationResult = Awaited<ReturnType<typeof addWorkerQueue>>;

export const useAddWorkerQueueKey = "AddWorkerQueue";
export const UseAddWorkerQueueKeyFn = (mutationKey?: Array<unknown>) => [useAddWorkerQueueKey, ...(mutationKey ?? [])];

export type SetWorkerConcurrencyLimitMutationResult = Awaited<ReturnType<typeof setWorkerConcurrencyLimit>>;

export const useSetWorkerConcurrencyLimitKey = "SetWorkerConcurrencyLimit";
export const UseSetWorkerConcurrencyLimitKeyFn = (mutationKey?: Array<unknown>) => [useSetWorkerConcurrencyLimitKey, ...(mutationKey ?? [])];
