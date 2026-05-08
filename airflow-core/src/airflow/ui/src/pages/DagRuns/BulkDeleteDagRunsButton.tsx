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

import type { DAGRunResponse } from "openapi/requests/types.gen";
import { ErrorAlert } from "src/components/ErrorAlert";
import { Dialog } from "src/components/ui";
import { useBulkDagRuns } from "src/queries/useBulkDagRuns";

type Props = {
  readonly clearSelections: VoidFunction;
  readonly selectedDagRuns: Array<DAGRunResponse>;
};

const BulkDeleteDagRunsButton = ({ clearSelections, selectedDagRuns }: Props) => {
  const { t: translate } = useTranslation();
  const { onClose, onOpen, open } = useDisclosure();
  const { bulkDelete, error, isPending } = useBulkDagRuns({
    clearSelections,
    onSuccessConfirm: onClose,
  });

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

            <Box maxH="320px" overflowY="auto">
              <VStack align="stretch" gap={1}>
                {selectedDagRuns.map((dagRun) => (
                  <Text fontSize="sm" key={`${dagRun.dag_id}:${dagRun.dag_run_id}`}>
                    <Text as="span" color="fg.subtle">
                      {dagRun.dag_id}
                    </Text>{" "}
                    / {dagRun.dag_run_id}
                  </Text>
                ))}
              </VStack>
            </Box>

            <ErrorAlert error={error} />
            <Flex justifyContent="end" mt={3}>
              <Button
                colorPalette="danger"
                loading={isPending}
                onClick={() => {
                  void bulkDelete(selectedDagRuns);
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
