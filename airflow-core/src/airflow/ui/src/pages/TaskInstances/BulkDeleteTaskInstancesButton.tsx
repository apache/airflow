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
import { Box, Button, Flex, Heading, Text, useDisclosure, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiTrash2 } from "react-icons/fi";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { getColumns } from "src/components/ActionAccordion/columns";
import { DataTable } from "src/components/DataTable";
import { ErrorAlert } from "src/components/ErrorAlert";
import { Accordion, Dialog } from "src/components/ui";
import { useBulkTaskInstances } from "src/queries/useBulkTaskInstances";

type Props = {
  readonly clearSelections: VoidFunction;
  readonly selectedTaskInstances: Array<TaskInstanceResponse>;
};

const BulkDeleteTaskInstancesButton = ({ clearSelections, selectedTaskInstances }: Props) => {
  const { t: translate } = useTranslation();
  const { onClose, onOpen, open } = useDisclosure();
  const { bulkAction, error, isPending } = useBulkTaskInstances({
    clearSelections,
    onSuccessConfirm: onClose,
  });

  const columns = getColumns(translate);

  // Group by dag_run_id for display
  const byRunId = new Map<string, Array<TaskInstanceResponse>>();

  for (const ti of selectedTaskInstances) {
    const group = byRunId.get(ti.dag_run_id) ?? [];

    group.push(ti);
    byRunId.set(ti.dag_run_id, group);
  }

  const isGrouped = byRunId.size > 1;

  return (
    <>
      <Button colorPalette="danger" onClick={onOpen} size="sm" variant="outline">
        <FiTrash2 />
        {translate("dags:runAndTaskActions.delete.button", { type: translate("taskInstance_other") })}
      </Button>

      <Dialog.Root onOpenChange={onClose} open={open} size="xl">
        <Dialog.Content backdrop>
          <Dialog.Header>
            <VStack align="start" gap={4}>
              <Heading size="xl">
                {translate("dags:runAndTaskActions.delete.dialog.title", {
                  type: translate("taskInstance_other"),
                })}
              </Heading>
            </VStack>
          </Dialog.Header>

          <Dialog.CloseTrigger />
          <Dialog.Body width="full">
            <Text color="fg.subtle" fontSize="sm" mb={4}>
              {translate("dags:runAndTaskActions.delete.dialog.warning", {
                type: translate("taskInstance_other"),
              })}
            </Text>

            <Box maxH="400px" overflowY="auto">
              {isGrouped ? (
                <Accordion.Root collapsible multiple variant="enclosed">
                  {[...byRunId.entries()].map(([runId, tis]) => (
                    <Accordion.Item key={runId} value={runId}>
                      <Accordion.ItemTrigger>
                        <Text fontSize="sm" fontWeight="semibold">
                          {translate("runId")}: {runId}{" "}
                          <Text as="span" color="fg.subtle" fontWeight="normal">
                            ({tis.length})
                          </Text>
                        </Text>
                      </Accordion.ItemTrigger>
                      <Accordion.ItemContent>
                        <DataTable
                          columns={columns}
                          data={tis}
                          displayMode="table"
                          modelName="common:taskInstance"
                          total={tis.length}
                        />
                      </Accordion.ItemContent>
                    </Accordion.Item>
                  ))}
                </Accordion.Root>
              ) : (
                <DataTable
                  columns={columns}
                  data={selectedTaskInstances}
                  displayMode="table"
                  modelName="common:taskInstance"
                  total={selectedTaskInstances.length}
                />
              )}
            </Box>

            <ErrorAlert error={error} />
            <Flex justifyContent="end" mt={3}>
              <Button
                colorPalette="danger"
                loading={isPending}
                onClick={() => {
                  bulkAction({
                    actions: [
                      {
                        action: "delete" as const,
                        action_on_non_existence: "skip",
                        entities: selectedTaskInstances.map((ti) => ({
                          dag_id: ti.dag_id,
                          dag_run_id: ti.dag_run_id,
                          map_index: ti.map_index,
                          task_id: ti.task_id,
                        })),
                      },
                    ],
                  });
                }}
              >
                <FiTrash2 />
                <Text fontWeight="bold">{translate("modal.confirm")}</Text>
              </Button>
            </Flex>
          </Dialog.Body>
        </Dialog.Content>
      </Dialog.Root>
    </>
  );
};

export default BulkDeleteTaskInstancesButton;
