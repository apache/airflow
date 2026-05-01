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
import { useSearchParams } from "react-router-dom";

import { SearchParamsKeys } from "src/constants/searchParams";

import type { GraphFilterValues } from "../useGraphFilteredNodes";

type GraphSearchParams = {
  depth: number | undefined;
  filterRoot: string | undefined;
  graphFilters: GraphFilterValues;
  hasActiveFilter: boolean;
  includeDownstream: boolean;
  includeUpstream: boolean;
};

export const useGraphSearchParams = (): GraphSearchParams => {
  const [searchParams] = useSearchParams();

  const filterRoot = searchParams.get("root") ?? undefined;
  const includeUpstream = searchParams.get("upstream") === "true";
  const includeDownstream = searchParams.get("downstream") === "true";
  const depthParam = searchParams.get("depth");
  const depth = depthParam !== null && depthParam !== "" ? parseInt(depthParam, 10) : undefined;
  const hasActiveFilter = includeUpstream || includeDownstream;

  const durationParam = searchParams.get(SearchParamsKeys.GRAPH_DURATION_GTE);
  const mapIndexParam = searchParams.get(SearchParamsKeys.GRAPH_MAP_INDEX);
  const durationVal = durationParam === null ? Number.NaN : Number(durationParam);
  const mapIndexVal = mapIndexParam === null ? Number.NaN : Number(mapIndexParam);

  const graphFilters: GraphFilterValues = {
    durationThreshold: Number.isNaN(durationVal) ? undefined : durationVal,
    mapIndex: Number.isNaN(mapIndexVal) ? undefined : mapIndexVal,
    selectedOperators: searchParams.getAll(SearchParamsKeys.GRAPH_OPERATOR),
    selectedStates: searchParams.getAll(SearchParamsKeys.GRAPH_TASK_STATE),
    selectedTaskGroups: searchParams.getAll(SearchParamsKeys.GRAPH_TASK_GROUP),
  };

  return { depth, filterRoot, graphFilters, hasActiveFilter, includeDownstream, includeUpstream };
};
