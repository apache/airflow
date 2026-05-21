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

import {
  useAssetStateServiceGetAssetStateKey,
  useAssetStateServiceListAssetStatesKey,
} from "openapi/queries";
import type {
  AssetStateCollectionResponse,
  AssetStateResponse,
  ClearAssetStateData,
  DeleteAssetStateData,
  GetAssetStateData,
  ListAssetStatesData,
  SetAssetStateData,
} from "openapi/requests";
import { AssetStateService } from "openapi/requests";

/** Stable key for query invalidation. */
export const UseListAssetStatesKey = useAssetStateServiceListAssetStatesKey;
export const UseGetAssetStateKey = useAssetStateServiceGetAssetStateKey;

// ---------------------------------------------------------------------------
// Read
// ---------------------------------------------------------------------------

export const useListAssetStates = <TData = AssetStateCollectionResponse>(
  params: ListAssetStatesData,
  queryKey?: Array<unknown>,
  options?: Omit<UseQueryOptions<TData>, "queryFn" | "queryKey">,
) =>
  useQuery<TData>({
    queryFn: () => AssetStateService.listAssetStates(params) as unknown as TData,
    queryKey: [UseListAssetStatesKey, params, ...(queryKey ?? [])],
    ...options,
  });

export const useGetAssetState = <TData = AssetStateResponse>(
  params: GetAssetStateData,
  queryKey?: Array<unknown>,
  options?: Omit<UseQueryOptions<TData>, "queryFn" | "queryKey">,
) =>
  useQuery<TData>({
    queryFn: () => AssetStateService.getAssetState(params) as unknown as TData,
    queryKey: [UseGetAssetStateKey, params, ...(queryKey ?? [])],
    ...options,
  });

// ---------------------------------------------------------------------------
// Write / Delete
// ---------------------------------------------------------------------------

export const useSetAssetState = (
  options?: Omit<UseMutationOptions<undefined, unknown, SetAssetStateData>, "mutationFn">,
) => {
  const queryClient = useQueryClient();

  return useMutation<undefined, unknown, SetAssetStateData>({
    ...options,
    mutationFn: (data) => AssetStateService.setAssetState(data) as unknown as Promise<undefined>,
    onSuccess: async (...args) => {
      await queryClient.invalidateQueries({ queryKey: [UseListAssetStatesKey] });
      await options?.onSuccess?.(...args);
    },
  });
};

export const useDeleteAssetState = (
  options?: Omit<UseMutationOptions<undefined, unknown, DeleteAssetStateData>, "mutationFn">,
) => {
  const queryClient = useQueryClient();

  return useMutation<undefined, unknown, DeleteAssetStateData>({
    ...options,
    mutationFn: (data) => AssetStateService.deleteAssetState(data) as unknown as Promise<undefined>,
    onSuccess: async (...args) => {
      await queryClient.invalidateQueries({ queryKey: [UseListAssetStatesKey] });
      await options?.onSuccess?.(...args);
    },
  });
};

export const useClearAssetState = (
  options?: Omit<UseMutationOptions<undefined, unknown, ClearAssetStateData>, "mutationFn">,
) => {
  const queryClient = useQueryClient();

  return useMutation<undefined, unknown, ClearAssetStateData>({
    ...options,
    mutationFn: (data) => AssetStateService.clearAssetState(data) as unknown as Promise<undefined>,
    onSuccess: async (...args) => {
      await queryClient.invalidateQueries({ queryKey: [UseListAssetStatesKey] });
      await options?.onSuccess?.(...args);
    },
  });
};
