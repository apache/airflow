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
import { Box, Field, Heading, Stack, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import type {
  DAGRunResponse,
  TaskInstanceCollectionResponse,
  TaskInstanceResponse,
} from "openapi/requests/types.gen";

import { Accordion } from "src/system-components";

import EditableMarkdown from "src/components/TriggerDag/EditableMarkdown";

import { formatNumber } from "src/utils";

import { DataTable } from "../DataTable";
import { getColumns, type RowSelection } from "./columns";

type Props = {
  readonly affectedTasks?: TaskInstanceCollectionResponse;
  readonly groupByRunId?: boolean;
  readonly note: DAGRunResponse["note"];
  readonly selection?: RowSelection;
  readonly setNote: (value: string) => void;
};

const TasksTable = ({
  noRowsMessage,
  selection,
  tasks,
}: {
  readonly noRowsMessage: string;
  readonly selection?: RowSelection;
  readonly tasks: Array<TaskInstanceResponse>;
}) => {
  const { t: translate } = useTranslation();
  const columns = getColumns(translate, selection);

  return (
    <DataTable
      columns={columns}
      data={tasks}
      displayMode="table"
      hideRowCountHeading
      modelName="common:taskInstance"
      noRowsMessage={noRowsMessage}
      total={tasks.length}
    />
  );
};

// Table is in memory, pagination and sorting are disabled.
// TODO: Make a front-end only unconnected table component with client side ordering and pagination
const ActionAccordion = ({ affectedTasks, groupByRunId = false, note, selection, setNote }: Props) => {
  const showTaskSection = affectedTasks !== undefined;
  const { i18n, t: translate } = useTranslation();

  // Group task instances by dag_run_id when requested
  const runGroups = (() => {
    if (!groupByRunId || !affectedTasks) {
      return undefined;
    }

    const map = new Map<string, Array<TaskInstanceResponse>>();

    for (const ti of affectedTasks.task_instances) {
      const group = map.get(ti.dag_run_id) ?? [];

      group.push(ti);
      map.set(ti.dag_run_id, group);
    }

    return map;
  })();

  // Only group when there are actually multiple run IDs
  const shouldGroup = groupByRunId && runGroups !== undefined && runGroups.size > 1;

  return (
    <VStack align="stretch" gap={4}>
      {showTaskSection ? (
        <Box>
          <Heading mb={2} size="lg">
            {translate("dags:runAndTaskActions.affectedTasks.title", {
              count: affectedTasks.total_entries ?? 0,
            })}
          </Heading>
          <Box borderRadius="md" borderWidth={1} maxH="400px" overflowY="auto">
            {shouldGroup ? (
              <Accordion.Root collapsible defaultValue={[...runGroups.keys()]} multiple variant="plain">
                {[...runGroups.entries()].map(([runId, tis]) => (
                  <Accordion.Item key={runId} value={runId}>
                    <Accordion.ItemTrigger px={2} py={1}>
                      <Text fontSize="sm" fontWeight="semibold">
                        {translate("runId")}: {runId}{" "}
                        <Text as="span" color="fg.subtle" fontWeight="normal">
                          ({formatNumber(tis.length, i18n.language)})
                        </Text>
                      </Text>
                    </Accordion.ItemTrigger>
                    <Accordion.ItemContent>
                      <TasksTable
                        noRowsMessage={translate("dags:runAndTaskActions.affectedTasks.noItemsFound")}
                        selection={selection}
                        tasks={tis}
                      />
                    </Accordion.ItemContent>
                  </Accordion.Item>
                ))}
              </Accordion.Root>
            ) : (
              <TasksTable
                noRowsMessage={translate("dags:runAndTaskActions.affectedTasks.noItemsFound")}
                selection={selection}
                tasks={affectedTasks.task_instances}
              />
            )}
          </Box>
        </Box>
      ) : undefined}
      <Field.Root orientation="horizontal">
        <Stack>
          <Field.Label fontSize="md" style={{ flexBasis: "30%" }}>
            {translate("note.label")}
          </Field.Label>
        </Stack>
        <Stack css={{ flexBasis: "70%" }}>
          <EditableMarkdown
            field={{ onChange: setNote, value: note ?? "" }}
            placeholder={translate("note.placeholder")}
          />
        </Stack>
      </Field.Root>
    </VStack>
  );
};

export default ActionAccordion;
