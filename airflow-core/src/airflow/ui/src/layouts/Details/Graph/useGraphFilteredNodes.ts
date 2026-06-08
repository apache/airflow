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
import type { Node as ReactFlowNode } from "@xyflow/react";
import { useMemo } from "react";

import type { CustomNodeProps } from "src/components/Graph/reactflowUtils";

type GraphFilterValues = {
  durationThreshold?: number;
  mapIndex?: number;
  selectedOperators: Array<string>;
  selectedStates: Array<string>;
  selectedTaskGroups: Array<string>;
};

const hasActiveFilters = (filters: GraphFilterValues): boolean =>
  filters.selectedOperators.length > 0 ||
  filters.selectedTaskGroups.length > 0 ||
  filters.selectedStates.length > 0 ||
  filters.durationThreshold !== undefined ||
  filters.mapIndex !== undefined;

const getTaskDurationSeconds = (
  minStartDate: string | null | undefined,
  maxEndDate: string | null | undefined,
): number | undefined => {
  if (
    minStartDate === null ||
    minStartDate === undefined ||
    maxEndDate === null ||
    maxEndDate === undefined
  ) {
    return undefined;
  }
  const start = new Date(minStartDate).getTime();
  const end = new Date(maxEndDate).getTime();

  if (Number.isNaN(start) || Number.isNaN(end)) {
    return undefined;
  }

  return (end - start) / 1000;
};

const isNodeFiltered = (node: ReactFlowNode<CustomNodeProps>, filters: GraphFilterValues): boolean => {
  if (node.type !== "task") {
    return false;
  }

  const { isMapped, operator, taskInstance } = node.data;

  if (
    filters.selectedOperators.length > 0 &&
    (operator === undefined || operator === null || !filters.selectedOperators.includes(operator))
  ) {
    return true;
  }

  if (filters.selectedTaskGroups.length > 0) {
    const matchesGroup = filters.selectedTaskGroups.some(
      (group) => node.id === group || node.id.startsWith(`${group}.`),
    );

    if (!matchesGroup) {
      return true;
    }
  }

  if (filters.selectedStates.length > 0) {
    const state = taskInstance?.state ?? "none";

    if (!filters.selectedStates.includes(state)) {
      return true;
    }
  }

  if (filters.mapIndex !== undefined) {
    if (!isMapped || taskInstance === undefined) {
      return true;
    }
    const total = Object.values(taskInstance.child_states ?? {}).reduce(
      (acc, currentValue) => acc + currentValue,
      0,
    );

    if (filters.mapIndex >= total) {
      return true;
    }
  }

  if (filters.durationThreshold !== undefined) {
    const duration = getTaskDurationSeconds(taskInstance?.min_start_date, taskInstance?.max_end_date);

    if (duration === undefined || duration < filters.durationThreshold) {
      return true;
    }
  }

  return false;
};

export const useGraphFilteredNodes = (
  nodes: Array<ReactFlowNode<CustomNodeProps>> | undefined,
  filters: GraphFilterValues,
): Array<ReactFlowNode<CustomNodeProps>> | undefined =>
  useMemo(() => {
    if (nodes === undefined) {
      return undefined;
    }

    if (!hasActiveFilters(filters)) {
      return nodes;
    }

    return nodes.map((node) => ({
      ...node,
      data: {
        ...node.data,
        isFiltered: isNodeFiltered(node, filters),
      },
    }));
  }, [nodes, filters]);

export type { GraphFilterValues };
