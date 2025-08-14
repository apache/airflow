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
import { Heading, Skeleton, Box, HStack } from "@chakra-ui/react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";

import { useTaskServiceGetTasks } from "openapi/queries";
import type { TaskResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import type { CardDef } from "src/components/DataTable/types";
import { ErrorAlert } from "src/components/ErrorAlert";
import { AttrSelectFilter } from "src/pages/Dag/Tasks/AttrSelectFilter.tsx";
import { ResetButton } from "src/pages/DagsList/DagsFilters/ResetButton.tsx";

import { TaskCard } from "./TaskCard";

const cardDef = (dagId: string): CardDef<TaskResponse> => ({
  card: ({ row }) => <TaskCard dagId={dagId} task={row} />,
  meta: {
    customSkeleton: <Skeleton height="120px" width="100%" />,
  },
});

const getFilterCount: ({ selectedOperator }: { selectedOperator: string | null | undefined }) => number = ({
  selectedOperator,
}) => {
  let count = 0;

  if (Boolean(selectedOperator)) {
    count += 1;
  }

  return count;
};

export const Tasks = () => {
  const { t: translate } = useTranslation();
  const { dagId = "" } = useParams();
  const [selectedOperator, setSelectedOperator] = useState<string | undefined>(undefined);
  const {
    data,
    error: tasksError,
    isFetching,
    isLoading,
  } = useTaskServiceGetTasks({
    dagId,
  });

  const filterTasks = (tasks: Array<TaskResponse>, operatorName: string | undefined) =>
    // debugger;
    Boolean(operatorName) ? tasks.filter((task: TaskResponse) => task.operator_name === operatorName) : tasks;

  const onClearFilters = () => {
    setSelectedOperator(undefined);
  };

  const handleOperatorSelect = (value: { value: Array<string> }) => {
    setSelectedOperator(value.value[0] as string | undefined);
  };
  const operatorNames: Array<string> =
    data?.tasks.map((task) => task.operator_name).filter((item) => item !== null) ?? [];
  const filterCount = getFilterCount({ selectedOperator });

  return (
    <Box>
      <ErrorAlert error={tasksError} />
      <Heading my={1} size="md">
        {translate("task", { count: data?.total_entries ?? 0 })}
      </Heading>

      <HStack justifyContent="space-between">
        <AttrSelectFilter
          handleSelect={handleOperatorSelect}
          placeholderText={translate("selectOperator")}
          selectedValue={selectedOperator}
          values={operatorNames}
        />

        <Box>
          <ResetButton filterCount={filterCount} onClearFilters={onClearFilters} />
        </Box>
      </HStack>

      <DataTable
        cardDef={cardDef(dagId)}
        columns={[]}
        data={filterTasks(data ? data.tasks : [], selectedOperator)}
        displayMode="card"
        isFetching={isFetching}
        isLoading={isLoading}
        modelName={translate("task_one")}
        total={data ? data.total_entries : 0}
      />
    </Box>
  );
};
