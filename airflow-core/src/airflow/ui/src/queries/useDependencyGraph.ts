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

export const useDependencyGraph = (
  nodeId: string,
  options?: {
    dependencyType?: "data" | "scheduling";
  } & Omit<UseQueryOptions<BaseGraphResponse, unknown>, "queryFn" | "queryKey">,
) => {
  const queryClient = useQueryClient();
  const { dependencyType, ...queryOptions } = options ?? {};

  const query = useDependenciesServiceGetDependencies(
    {
      dependencyType,
      nodeId,
    },
    undefined,
    queryOptions,
  );

  // Update the queries for all connected assets and dags so we save an API request
  // Include dependencyType in the cache key to handle scheduling vs data dependencies separately
  query.data?.nodes.forEach((node) => {
    const key = UseDependenciesServiceGetDependenciesKeyFn({ dependencyType, nodeId: node.id });
    const queryData = queryClient.getQueryData(key);

    if (!Boolean(queryData)) {
      queryClient.setQueryData(key, query.data);
    }
  });

  return query;
};
