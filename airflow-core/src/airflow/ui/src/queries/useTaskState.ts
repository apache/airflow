/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import type { UseMutationOptions, UseQueryOptions } from "@tanstack/react-query";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { useTaskStateServiceGetTaskStateKey, useTaskStateServiceListTaskStatesKey } from "openapi/queries";
import type {
  ClearTaskStateData,
  DeleteTaskStateData,
  GetTaskStateData,
  ListTaskStatesData,
  SetTaskStateData,
  TaskStateCollectionResponse,
  TaskStateResponse,
} from "openapi/requests";
import { TaskStateService } from "openapi/requests";

/** Stable key for query invalidation — mirrors the generated `useXxxServiceYyyKey` pattern. */
export const UseListTaskStatesKey = useTaskStateServiceListTaskStatesKey;
export const UseGetTaskStateKey = useTaskStateServiceGetTaskStateKey;

// ---------------------------------------------------------------------------
// Read
// ---------------------------------------------------------------------------

export const useListTaskStates = <TData = TaskStateCollectionResponse>(
  params: ListTaskStatesData,
  queryKey?: Array<unknown>,
  options?: Omit<UseQueryOptions<TData>, "queryFn" | "queryKey">,
) =>
  useQuery<TData>({
    queryFn: () => TaskStateService.listTaskStates(params) as unknown as TData,
    queryKey: [UseListTaskStatesKey, params, ...(queryKey ?? [])],
    ...options,
  });

export const useGetTaskState = <TData = TaskStateResponse>(
  params: GetTaskStateData,
  queryKey?: Array<unknown>,
  options?: Omit<UseQueryOptions<TData>, "queryFn" | "queryKey">,
) =>
  useQuery<TData>({
    queryFn: () => TaskStateService.getTaskState(params) as unknown as TData,
    queryKey: [UseGetTaskStateKey, params, ...(queryKey ?? [])],
    ...options,
  });

// ---------------------------------------------------------------------------
// Write / Delete
// ---------------------------------------------------------------------------

export const useSetTaskState = (
  options?: Omit<UseMutationOptions<undefined, unknown, SetTaskStateData>, "mutationFn">,
) => {
  const queryClient = useQueryClient();

  return useMutation<undefined, unknown, SetTaskStateData>({
    ...options,
    mutationFn: (data) => TaskStateService.setTaskState(data) as unknown as Promise<undefined>,
    onSuccess: async (...args) => {
      await queryClient.invalidateQueries({ queryKey: [UseListTaskStatesKey] });
      await options?.onSuccess?.(...args);
    },
  });
};

export const useDeleteTaskState = (
  options?: Omit<UseMutationOptions<undefined, unknown, DeleteTaskStateData>, "mutationFn">,
) => {
  const queryClient = useQueryClient();

  return useMutation<undefined, unknown, DeleteTaskStateData>({
    ...options,
    mutationFn: (data) => TaskStateService.deleteTaskState(data) as unknown as Promise<undefined>,
    onSuccess: async (...args) => {
      await queryClient.invalidateQueries({ queryKey: [UseListTaskStatesKey] });
      await options?.onSuccess?.(...args);
    },
  });
};

export const useClearTaskState = (
  options?: Omit<UseMutationOptions<undefined, unknown, ClearTaskStateData>, "mutationFn">,
) => {
  const queryClient = useQueryClient();

  return useMutation<undefined, unknown, ClearTaskStateData>({
    ...options,
    mutationFn: (data) => TaskStateService.clearTaskState(data) as unknown as Promise<undefined>,
    onSuccess: async (...args) => {
      await queryClient.invalidateQueries({ queryKey: [UseListTaskStatesKey] });
      await options?.onSuccess?.(...args);
    },
  });
};
