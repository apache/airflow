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
} from "openapi/queries";
import type {
  TaskResponse,
  TaskInstanceResponse,
} from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import type { CardDef } from "src/components/DataTable/types";
import { ErrorAlert } from "src/components/ErrorAlert";

import { TaskCard } from "./TaskCard";

const cardDef = (
  taskInstances?: Array<TaskInstanceResponse>,
): CardDef<TaskResponse> => ({
  card: ({ row }) => (
    <TaskCard
      task={row}
      taskInstance={
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
  const { dagId } = useParams();
  const {
    data,
    error: TasksError,
    isFetching,
    isLoading,
  } = useTaskServiceGetTasks({
    dagId: dagId ?? "",
  });

  // TODO : Get the latest dag run and then pass it to dagRunId for better querying
  const { data: TaskInstancesResponse } =
    useTaskInstanceServiceGetTaskInstances({
      dagId: dagId ?? "",
      dagRunId: "~",
    });

  return (
    <Box>
      <ErrorAlert error={TasksError} />
      <Heading my={1} size="md">
        {data ? data.total_entries : 0} Tasks
      </Heading>
      <DataTable
        cardDef={cardDef(TaskInstancesResponse?.task_instances)}
        columns={[]}
        data={data ? data.tasks : []}
        displayMode="card"
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="Task"
        skeletonCount={undefined}
        total={data ? data.total_entries : 0}
      />
    </Box>
  );
};
