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
import { Box, Link } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";
import { useParams, useSearchParams } from "react-router-dom";
import { Link as RouterLink } from "react-router-dom";

import { useTaskServiceGetTasks } from "openapi/queries";
import type { TaskResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { ErrorAlert } from "src/components/ErrorAlert";
import { TruncatedText } from "src/components/TruncatedText";
import { SearchParamsKeys } from "src/constants/searchParams.ts";
import { TaskFilters } from "src/pages/Dag/Tasks/TaskFilters/TaskFilters.tsx";

type TaskRow = { row: { original: TaskResponse } };

const createColumns = ({
  dagId,
  translate,
}: {
  dagId: string;
  translate: TFunction;
}): Array<ColumnDef<TaskResponse>> => [
  {
    accessorKey: "task_display_name",
    cell: ({ row: { original } }: TaskRow) => (
      <Link asChild color="fg.info" fontWeight="bold">
        <RouterLink to={`/dags/${dagId}/tasks/${original.task_id}`}>
          <TruncatedText text={original.task_display_name ?? original.task_id ?? ""} />
        </RouterLink>
      </Link>
    ),
    enableSorting: false,
    header: translate("common:taskId"),
  },
  {
    accessorKey: "trigger_rule",
    enableSorting: false,
    header: translate("common:task.triggerRule"),
  },
  {
    accessorKey: "operator_name",
    enableSorting: false,
    header: translate("common:task.operator"),
  },
  {
    accessorKey: "retries",
    enableSorting: false,
    header: translate("tasks:retries"),
  },
  {
    accessorKey: "is_mapped",
    enableSorting: false,
    header: translate("tasks:mapped"),
  },
];

export const Tasks = () => {
  const { dagId = "" } = useParams();
  const { MAPPED, NAME_PATTERN, OPERATOR, RETRIES, TRIGGER_RULE } = SearchParamsKeys;
  const [searchParams] = useSearchParams();
  const selectedOperators = searchParams.getAll(OPERATOR);
  const selectedTriggerRules = searchParams.getAll(TRIGGER_RULE);
  const selectedRetries = searchParams.getAll(RETRIES);
  const selectedMapped = searchParams.get(MAPPED) ?? undefined;
  const namePattern = searchParams.get(NAME_PATTERN) ?? undefined;

  const { t: translate } = useTranslation(["tasks", "common"]);

  const columns = createColumns({ dagId, translate });

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
        columns={columns}
        data={filteredTasks}
        displayMode="card"
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="common:task"
        total={data ? data.total_entries : 0}
      />
    </Box>
  );
};
