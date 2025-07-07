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
import { useQueryClient, type UseQueryOptions } from "@tanstack/react-query";

import {
  useDependenciesServiceGetDependencies,
  UseDependenciesServiceGetDependenciesKeyFn,
} from "openapi/queries";
import type { BaseGraphResponse } from "openapi/requests/types.gen";

/**
 * Hook to fetch the dependency graph for a list of asset ids.
 * Uses the backend endpoint that accepts node_ids as an exploded array (node_ids=foo&node_ids=bar).
 * Caches the result by ids.
 */
export const useMultiDependencyGraph = (
  assetIds: Array<string>,
  options?: Omit<UseQueryOptions<BaseGraphResponse, unknown>, "queryFn" | "queryKey">,
) => {
  const queryClient = useQueryClient();

  // Always send nodeIds as an array of asset:... strings
  const nodeIdsParam = assetIds.map((id) => (id.startsWith("asset:") ? id : `asset:${id}`));

  const query = useDependenciesServiceGetDependencies({ nodeIds: nodeIdsParam }, undefined, options);

  if (query.data) {
    const key = UseDependenciesServiceGetDependenciesKeyFn({ nodeIds: nodeIdsParam });
    const queryData = queryClient.getQueryData(key);

    if (!Boolean(queryData)) {
      queryClient.setQueryData(key, query.data);
    }
  }

  return query;
};
