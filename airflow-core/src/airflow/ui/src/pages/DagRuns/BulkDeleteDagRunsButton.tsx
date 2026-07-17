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
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";
import { FiTrash2 } from "react-icons/fi";

import type { DAGRunResponse } from "openapi/requests/types.gen";
import { ActionErrors } from "src/components/ActionErrors";
import { DataTable } from "src/components/DataTable";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { Accordion, Dialog } from "src/components/ui";
import { useBulkDeleteDagRuns } from "src/queries/useBulkDeleteDagRuns";

type Props = {
  readonly deselectKeys: (keys: Array<string>) => void;
  readonly selectedDagRuns: Array<DAGRunResponse>;
};

const getColumns = (translate: TFunction): Array<ColumnDef<DAGRunResponse>> => [
  {
    accessorKey: "dag_run_id",
    cell: ({ row: { original } }) => <Text>{original.dag_run_id}</Text>,
    enableSorting: false,
    header: translate("dagRunId"),
  },
  {
    accessorKey: "state",
    cell: ({ row: { original } }) => (
      <StateBadge state={original.state}>{translate(`common:states.${original.state}`)}</StateBadge>
    ),
    enableSorting: false,
    header: translate("state"),
  },
  {
    accessorKey: "run_after",
    cell: ({ row: { original } }) => <Time datetime={original.run_after} />,
    enableSorting: false,
    header: translate("dagRun.runAfter"),
  },
];

const BulkDeleteDagRunsButton = ({ deselectKeys, selectedDagRuns }: Props) => {
  const { t: translate } = useTranslation(["common", "dags"]);
  const { onClose, onOpen, open } = useDisclosure();
  const { bulkAction, data, error, isPending } = useBulkDeleteDagRuns({
    deselectKeys,
    onSuccessConfirm: onClose,
  });

  const columns = getColumns(translate);

  const byDagId = new Map<string, Array<DAGRunResponse>>();

  for (const dagRun of selectedDagRuns) {
    const group = byDagId.get(dagRun.dag_id) ?? [];

    group.push(dagRun);
    byDagId.set(dagRun.dag_id, group);
  }

  const isGrouped = byDagId.size > 1;

  return (
    <>
      <Button colorPalette="danger" onClick={onOpen} size="sm" variant="outline">
        <FiTrash2 />
        {translate("dags:runAndTaskActions.delete.button", { type: translate("dagRun_other") })}
      </Button>

      <Dialog.Root onOpenChange={onClose} open={open} size="xl">
        <Dialog.Content backdrop>
          <Dialog.Header>
            <VStack align="start" gap={4}>
              <Heading size="xl">
                {translate("dags:runAndTaskActions.delete.dialog.title", {
                  type: translate("dagRun_other"),
                })}
              </Heading>
            </VStack>
          </Dialog.Header>

          <Dialog.CloseTrigger />
          <Dialog.Body width="full">
            <Text color="fg.subtle" fontSize="sm" mb={4}>
              {translate("dags:runAndTaskActions.delete.dialog.warning", {
                type: translate("dagRun_other"),
              })}
            </Text>

            <Box maxH="400px" overflowY="auto">
              {isGrouped ? (
                <Accordion.Root collapsible multiple variant="enclosed">
                  {[...byDagId.entries()].map(([dagId, dagRuns]) => (
                    <Accordion.Item key={dagId} value={dagId}>
                      <Accordion.ItemTrigger>
                        <Text fontSize="sm" fontWeight="semibold">
                          {translate("dagId")}: {dagId}{" "}
                          <Text as="span" color="fg.subtle" fontWeight="normal">
                            ({dagRuns.length})
                          </Text>
                        </Text>
                      </Accordion.ItemTrigger>
                      <Accordion.ItemContent>
                        <DataTable
                          columns={columns}
                          data={dagRuns}
                          displayMode="table"
                          modelName="common:dagRun"
                          total={dagRuns.length}
                        />
                      </Accordion.ItemContent>
                    </Accordion.Item>
                  ))}
                </Accordion.Root>
              ) : (
                <DataTable
                  columns={columns}
                  data={selectedDagRuns}
                  displayMode="table"
                  modelName="common:dagRun"
                  total={selectedDagRuns.length}
                />
              )}
            </Box>

            <ActionErrors actionResponse={data?.delete} error={error} />
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
                        entities: selectedDagRuns.map((dagRun) => ({
                          dag_id: dagRun.dag_id,
                          dag_run_id: dagRun.dag_run_id,
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

export default BulkDeleteDagRunsButton;
