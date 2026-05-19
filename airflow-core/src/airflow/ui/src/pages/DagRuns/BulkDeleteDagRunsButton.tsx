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
import { Box, Button, Flex, Heading, Stack, Text, useDisclosure, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiTrash2 } from "react-icons/fi";

import type { DAGRunResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { ErrorAlert } from "src/components/ErrorAlert";
import { Accordion, Alert, Dialog } from "src/components/ui";
import { useBulkDeleteDagRuns } from "src/queries/useBulkDeleteDagRuns";

import { getBulkDagRunsColumns } from "./bulkDagRunsColumns";

type Props = {
  readonly clearSelections: VoidFunction;
  readonly selectedDagRuns: Array<DAGRunResponse>;
};

const BulkDeleteDagRunsButton = ({ clearSelections, selectedDagRuns }: Props) => {
  const { t: translate } = useTranslation(["common", "dags"]);
  const { onClose, onOpen, open } = useDisclosure();
  const { actionErrors, bulkDelete, error, isPending, reset } = useBulkDeleteDagRuns({
    clearSelections,
    onSuccessConfirm: onClose,
  });

  const handleClose = () => {
    reset();
    onClose();
  };

  const columns = getBulkDagRunsColumns(translate);

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

      <Dialog.Root onOpenChange={handleClose} open={open} size="xl">
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

            <ErrorAlert error={error} />
            {actionErrors.length > 0 ? (
              <Stack gap={2} mt={3}>
                {actionErrors.map((actionError, index) => (
                  // eslint-disable-next-line react/no-array-index-key -- errors have no stable id
                  <Alert key={index} status="error" title={actionError.error} />
                ))}
              </Stack>
            ) : undefined}
            <Flex justifyContent="end" mt={3}>
              <Button
                colorPalette="danger"
                loading={isPending}
                onClick={() => {
                  bulkDelete(
                    selectedDagRuns.map((dagRun) => ({
                      dag_id: dagRun.dag_id,
                      dag_run_id: dagRun.dag_run_id,
                    })),
                  );
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
