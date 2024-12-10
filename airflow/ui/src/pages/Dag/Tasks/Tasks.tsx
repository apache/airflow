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
import { Heading, Skeleton, Box } from "@chakra-ui/react";
import { useParams } from "react-router-dom";

import {
  useTaskServiceGetTasks,
  useTaskInstanceServiceGetTaskInstances,
  useDagsServiceRecentDagRuns,
} from "openapi/queries";
import type {
  TaskResponse,
  TaskInstanceResponse,
} from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import type { CardDef } from "src/components/DataTable/types";
import { ErrorAlert } from "src/components/ErrorAlert";
import { pluralize } from "src/utils";

import { TaskCard } from "./TaskCard";

const cardDef = (
  dagId: string,
  taskInstances?: Array<TaskInstanceResponse>,
): CardDef<TaskResponse> => ({
  card: ({ row }) => (
    <TaskCard
      dagId={dagId}
      task={row}
      taskInstances={
        taskInstances
          ? taskInstances.filter(
              (instance: TaskInstanceResponse) =>
                instance.task_id === row.task_id,
            )
          : []
      }
    />
  ),
  meta: {
    customSkeleton: <Skeleton height="120px" width="100%" />,
  },
});

export const Tasks = () => {
  const { dagId = "" } = useParams();
  const {
    data,
    error: tasksError,
    isFetching,
    isLoading,
  } = useTaskServiceGetTasks({
    dagId,
  });

  // TODO: Replace dagIdPattern with dagId once supported for better matching
  const { data: runsData } = useDagsServiceRecentDagRuns(
    { dagIdPattern: dagId, dagRunsLimit: 14 },
    undefined,
    {
      enabled: Boolean(dagId),
    },
  );

  const runs =
    runsData?.dags.find((dagWithRuns) => dagWithRuns.dag_id === dagId)
      ?.latest_dag_runs ?? [];

  // TODO: Revisit this endpoint since only 100 task instances are returned and
  // only duration is calculated with other attributes unused.
  const { data: taskInstancesResponse } =
    useTaskInstanceServiceGetTaskInstances(
      {
        dagId,
        dagRunId: "~",
        logicalDateGte: runs.at(-1)?.logical_date ?? "",
      },
      undefined,
      { enabled: Boolean(runs[0]?.dag_run_id) },
    );

  return (
    <Box>
      <ErrorAlert error={tasksError} />
      <Heading my={1} size="md">
        {pluralize("Task", data ? data.total_entries : 0)}
      </Heading>
      <DataTable
        cardDef={cardDef(
          dagId,
          taskInstancesResponse?.task_instances.reverse(),
        )}
        columns={[]}
        data={data ? data.tasks : []}
        displayMode="card"
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="Task"
        total={data ? data.total_entries : 0} // Todo : Disable pagination?
      />
    </Box>
  );
};
