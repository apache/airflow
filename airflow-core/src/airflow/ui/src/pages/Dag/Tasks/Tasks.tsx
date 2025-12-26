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
import { Skeleton, Box } from "@chakra-ui/react";
import { useQuery } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { useParams, useSearchParams } from "react-router-dom";

import { useTaskServiceGetTasks } from "openapi/queries";
import { TaskInstanceService } from "openapi/requests/services.gen";
import type { TaskInstanceResponse, TaskResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import type { CardDef } from "src/components/DataTable/types";
import { ErrorAlert } from "src/components/ErrorAlert";
import { SearchParamsKeys } from "src/constants/searchParams.ts";
import { isStatePending, useAutoRefresh } from "src/utils";
import { TaskFilters } from "src/pages/Dag/Tasks/TaskFilters/TaskFilters.tsx";

import { TaskCard } from "./TaskCard";

const cardDef = (
  dagId: string,
  taskInstancesByTaskId: Record<string, Array<TaskInstanceResponse>>,
): CardDef<TaskResponse> => ({
  card: ({ row }) => (
    <TaskCard dagId={dagId} task={row} taskInstances={taskInstancesByTaskId[row.task_id ?? ""] ?? []} />
  ),
  meta: {
    customSkeleton: <Skeleton height="120px" width="100%" />,
  },
});

export const Tasks = () => {
  const { dagId = "" } = useParams();
  const { MAPPED, NAME_PATTERN, OPERATOR, RETRIES, TRIGGER_RULE } = SearchParamsKeys;
  const { t: translate } = useTranslation();
  const [searchParams] = useSearchParams();
  const selectedOperators = searchParams.getAll(OPERATOR);
  const selectedTriggerRules = searchParams.getAll(TRIGGER_RULE);
  const selectedRetries = searchParams.getAll(RETRIES);
  const selectedMapped = searchParams.get(MAPPED) ?? undefined;
  const namePattern = searchParams.get(NAME_PATTERN) ?? undefined;
  const refetchInterval = useAutoRefresh({ dagId });

  const {
    data,
    error: tasksError,
    isFetching,
    isLoading,
  } = useTaskServiceGetTasks({
    dagId,
  });

  // Extract task IDs for batch fetching task instances
  const taskIds = data?.tasks.map((task) => task.task_id ?? "").filter((id) => id) ?? [];

  // Batch fetch task instances for all tasks in one API call
  const {
    data: taskInstancesData,
    error: taskInstancesError,
    isFetching: isFetchingTaskInstances,
  } = useQuery({
    enabled: Boolean(dagId) && taskIds.length > 0,
    queryFn: () =>
      TaskInstanceService.getTaskInstancesBatch({
        dagId: "~",
        dagRunId: "~",
        requestBody: {
          dag_ids: [dagId],
          order_by: "-run_after",
          page_limit: taskIds.length * 14, // Fetch up to 14 instances per task
          task_ids: taskIds,
        },
      }),
    queryKey: ["taskInstancesBatch", dagId, taskIds],
    refetchInterval: (query) =>
      query.state.data?.task_instances.some((ti) => isStatePending(ti.state)) ? refetchInterval : false,
  });

  // Group task instances by task_id for efficient lookup
  const taskInstancesByTaskId = (taskInstancesData?.task_instances ?? []).reduce(
    (acc, ti) => {
      const taskId = ti.task_id;

      if (taskId) {
        if (!acc[taskId]) {
          acc[taskId] = [];
        }
        // Limit to 14 instances per task (matching original behavior)
        if (acc[taskId].length < 14) {
          acc[taskId].push(ti);
        }
      }

      return acc;
    },
    {} as Record<string, Array<TaskInstanceResponse>>,
  );

  const filterTasks = ({
    mapped,
    operatorNames,
    retryValues,
    tasks,
    triggerRuleNames,
  }: {
    mapped: string | undefined;
    operatorNames: Array<string>;
    retryValues: Array<string>;
    tasks: Array<TaskResponse>;
    triggerRuleNames: Array<string>;
  }) =>
    tasks.filter(
      (task) =>
        (operatorNames.length === 0 || operatorNames.includes(task.operator_name as string)) &&
        (triggerRuleNames.length === 0 || triggerRuleNames.includes(task.trigger_rule as string)) &&
        (retryValues.length === 0 || retryValues.includes(task.retries?.toString() as string)) &&
        (mapped === undefined || task.is_mapped?.toString() === mapped) &&
        (namePattern === undefined || task.task_display_name?.toString().includes(namePattern)),
    );

  const filteredTasks = filterTasks({
    mapped: selectedMapped,
    operatorNames: selectedOperators,
    retryValues: selectedRetries,
    tasks: data ? data.tasks : [],
    triggerRuleNames: selectedTriggerRules,
  });

  return (
    <Box>
      <ErrorAlert error={tasksError ?? taskInstancesError} />

      <TaskFilters tasksData={data} />

      <DataTable
        cardDef={cardDef(dagId, taskInstancesByTaskId)}
        columns={[]}
        data={filteredTasks}
        displayMode="card"
        isFetching={isFetching || isFetchingTaskInstances}
        isLoading={isLoading}
        modelName={translate("task_one")}
        total={data ? data.total_entries : 0}
      />
    </Box>
  );
};
