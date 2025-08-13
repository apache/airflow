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
import {Heading, Skeleton, Box, createListCollection, CollectionItem} from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";

import { useTaskServiceGetTasks } from "openapi/queries";
import type { TaskResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import type { CardDef } from "src/components/DataTable/types";
import { ErrorAlert } from "src/components/ErrorAlert";
import { useState } from "react";

import { TaskCard } from "./TaskCard";
import { Select } from "src/components/ui";

const cardDef = (dagId: string): CardDef<TaskResponse> => ({
  card: ({ row }) => <TaskCard dagId={dagId} task={row} />,
  meta: {
    customSkeleton: <Skeleton height="120px" width="100%" />,
  },
});

export const Tasks = () => {
  const { t: translate } = useTranslation();
  const { dagId = "" } = useParams();
  const [selectedOperator, setSelectedOperator] = useState("");
  const {
    data,
    error: tasksError,
    isFetching,
    isLoading,
  } = useTaskServiceGetTasks({
    dagId,
  });


  const operatorList = data?.tasks?.map((task) => ({label: task.operator_name, value: task.operator_name}))
  const totalOperatorList = [{ value: "", label: "All Operators" }, ...(operatorList ?? [])]
  const operatorCollection = createListCollection({items: totalOperatorList})

  const filterTasks = (tasks: Array<TaskResponse>) => {
    debugger;
    if (Boolean(selectedOperator)) {
      return tasks.filter((task: TaskResponse) => task.operator_name == selectedOperator)
    } else {
      return tasks
    }
  }

  const handleOperatorSelect = (value: CollectionItem) => {
    setSelectedOperator(value.value[0])
  }

  return (
    <Box>
      <ErrorAlert error={tasksError} />
      <Heading my={1} size="md">
        {translate("task", { count: data?.total_entries ?? 0 })}
      </Heading>

      <Select.Root
        collection={operatorCollection}
        maxW="200px"
        onValueChange={handleOperatorSelect}
        value={[selectedOperator ?? "all"]}
      >
        <Select.Trigger colorPalette="blue" isActive={Boolean("hi")} minW="max-content">
          <Select.ValueText width="auto">
            {() => selectedOperator}
          </Select.ValueText>
        </Select.Trigger>
        <Select.Content>
          {operatorCollection.items.map((option) => (
            <Select.Item item={option} key={option.label}>
              {option.label}
            </Select.Item>
          ))}
        </Select.Content>
      </Select.Root>

      <DataTable
        cardDef={cardDef(dagId)}
        columns={[]}
        data={filterTasks(data ? data.tasks : [])}
        displayMode="card"
        isFetching={isFetching}
        isLoading={isLoading}
        modelName={translate("task_one")}
        total={data ? data.total_entries : 0}
      />
    </Box>
  );
};
