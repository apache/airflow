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
import { AttrSelectFilterMulti } from "src/pages/Dag/Tasks/AttrSelectFilterMulti.tsx";
import { ResetButton } from "src/pages/DagsList/DagsFilters/ResetButton.tsx";

import { TaskCard } from "./TaskCard";

const cardDef = (dagId: string): CardDef<TaskResponse> => ({
  card: ({ row }) => <TaskCard dagId={dagId} task={row} />,
  meta: {
    customSkeleton: <Skeleton height="120px" width="100%" />,
  },
});

export const Tasks = () => {
  const { t: translate } = useTranslation();
  const { dagId = "" } = useParams();
  const [selectedOperators, setSelectedOperators] = useState<Array<string> | undefined>(undefined);
  const [selectedTriggerRules, setSelectedTriggerRules] = useState<Array<string> | undefined>(undefined);
  const [selectedRetryValues, setSelectedRetryValues] = useState<Array<string> | undefined>(undefined);
  const {
    data,
    error: tasksError,
    isFetching,
    isLoading,
  } = useTaskServiceGetTasks({
    dagId,
  });

  const onClearFilters = () => {
    setSelectedOperators(undefined);
    setSelectedTriggerRules(undefined);
  };

  const allOperatorNames: Array<string> = [
    ...new Set(data?.tasks.map((task) => task.operator_name).filter((item) => item !== null) ?? []),
  ];
  const allTriggerRules: Array<string> = [
    ...new Set(data?.tasks.map((task) => task.trigger_rule).filter((item) => item !== null) ?? []),
  ];
  const allRetryValues: Array<string> = [
    ...new Set(
      data?.tasks.map((task) => task.retries?.toString()).filter((item) => item !== undefined) ?? [],
    ),
  ];

  const filterTasks = ({
    operatorNames,
    retryValues,
    tasks,
    triggerRuleNames,
  }: {
    operatorNames: Array<string>;
    retryValues: Array<string>;
    tasks: Array<TaskResponse>;
    triggerRuleNames: Array<string>;
  }) =>
    tasks.filter(
      (task) =>
        (operatorNames.length === 0 || operatorNames.includes(task.operator_name as string)) &&
        (triggerRuleNames.length === 0 || triggerRuleNames.includes(task.trigger_rule as string)) &&
        (retryValues.length === 0 || retryValues.includes(task.retries?.toString() as string)),
    );

  const filteredTasks = filterTasks({
    operatorNames: selectedOperators ?? [],
    retryValues: selectedRetryValues ?? [],
    tasks: data ? data.tasks : [],
    triggerRuleNames: selectedTriggerRules ?? [],
  });

  return (
    <Box>
      <ErrorAlert error={tasksError} />
      <Heading my={1} size="md">
        {translate("task", { count: data?.total_entries ?? 0 })}
      </Heading>

      <HStack>
        <AttrSelectFilterMulti
          handleSelect={setSelectedOperators}
          placeholderText={translate("selectOperator")}
          selectedValues={selectedOperators}
          values={allOperatorNames}
        />
        <AttrSelectFilterMulti
          handleSelect={setSelectedTriggerRules}
          placeholderText={translate("selectTriggerRules")}
          selectedValues={selectedTriggerRules}
          values={allTriggerRules}
        />
        <AttrSelectFilterMulti
          handleSelect={setSelectedRetryValues}
          placeholderText={translate("selectRetryValues")}
          selectedValues={selectedRetryValues}
          values={allRetryValues}
        />
        <Box>
          <ResetButton filterCount={2} onClearFilters={onClearFilters} />
        </Box>
      </HStack>

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
