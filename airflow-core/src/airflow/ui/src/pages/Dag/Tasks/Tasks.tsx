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
import { useTranslation } from "react-i18next";
import { useParams, useSearchParams } from "react-router-dom";

import { useTaskServiceGetTasks } from "openapi/queries";
import type { TaskResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import type { CardDef } from "src/components/DataTable/types";
import { ErrorAlert } from "src/components/ErrorAlert";
import { SearchParamsKeys } from "src/constants/searchParams.ts";
import { TaskFilters } from "src/pages/Dag/Tasks/TaskFilters/TaskFilters.tsx";

import { TaskCard } from "./TaskCard";

const cardDef = (dagId: string): CardDef<TaskResponse> => ({
  card: ({ row }) => <TaskCard dagId={dagId} task={row} />,
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

  const {
    data,
    error: tasksError,
    isFetching,
    isLoading,
  } = useTaskServiceGetTasks({
    dagId,
  });

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
      <ErrorAlert error={tasksError} />

      <TaskFilters tasksData={data} />

      <DataTable
        cardDef={cardDef(dagId)}
        columns={[]}
        data={filteredTasks}
        displayMode="card"
        isFetching={isFetching}
        isLoading={isLoading}
        modelName={translate("task_one")}
        total={data ? data.total_entries : 0}
      />
    </Box>
  );
};
