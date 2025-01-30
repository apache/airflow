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

import { useTaskServiceGetTasks, useDagRunServiceGetDagRuns } from "openapi/queries";
import type { TaskResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import type { CardDef } from "src/components/DataTable/types";
import { ErrorAlert } from "src/components/ErrorAlert";
import { useConfig } from "src/queries/useConfig";
import { pluralize } from "src/utils";

import { TaskCard } from "./TaskCard";

const cardDef = (dagId: string, areActiveRuns?: boolean): CardDef<TaskResponse> => ({
  card: ({ row }) => <TaskCard areActiveRuns={areActiveRuns} dagId={dagId} task={row} />,
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

  const autoRefreshInterval = useConfig("auto_refresh_interval") as number;

  const { data: runsData } = useDagRunServiceGetDagRuns(
    {
      dagId,
      limit: 14,
      orderBy: "-logical_date",
      state: ["queued", "running"],
    },
    undefined,
    {
      refetchInterval: (query) =>
        Boolean(query.state.data?.dag_runs.length) ? autoRefreshInterval * 1000 : false,
    },
  );

  const runs = runsData?.dag_runs ?? [];

  return (
    <Box>
      <ErrorAlert error={tasksError} />
      <Heading my={1} size="md">
        {pluralize("Task", data ? data.total_entries : 0)}
      </Heading>
      <DataTable
        cardDef={cardDef(dagId, runs.length > 0)}
        columns={[]}
        data={data ? data.tasks : []}
        displayMode="card"
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="Task"
        total={data ? data.total_entries : 0}
      />
    </Box>
  );
};
