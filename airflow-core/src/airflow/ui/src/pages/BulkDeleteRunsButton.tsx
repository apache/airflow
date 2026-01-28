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
import { Button, Flex, Heading, Text, useDisclosure, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiTrash2 } from "react-icons/fi";

import { ErrorAlert } from "src/components/ErrorAlert";
import { Dialog } from "src/components/ui";
import { useBulkDeleteDagRuns, type SelectedRun } from "src/queries/useBulkDeleteDagRuns";

type BulkDeleteRunsButtonProps = {
  readonly onSuccessConfirm: () => void;
  readonly selectedRuns: Array<SelectedRun>;
};

const BulkDeleteRunsButton = ({ onSuccessConfirm, selectedRuns }: BulkDeleteRunsButtonProps) => {
  const { onClose, onOpen, open } = useDisclosure();
  const { t: translate } = useTranslation();

  const count = selectedRuns.length;

  const { bulkDelete, error, isDeleting } = useBulkDeleteDagRuns(() => {
    onClose();
    onSuccessConfirm();
  });

  return (
    <>
      <Button
        aria-label={translate("dags:runAndTaskActions.delete.button", { type: translate("dagRun_other") })}
        colorPalette="danger"
        disabled={count === 0}
        onClick={onOpen}
        size="sm"
        variant="outline"
      >
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
            <Text color="fg" fontSize="md" fontWeight="semibold" mb={4}>
              {/* Just show the count, like old resourceName did */}
              {translate("dags:runAndTaskActions.delete.dialog.warning", {
                type: translate("dagRun_other"),
              })}{" "}
              ({count} {translate("dagRun", { count })})
            </Text>

            <ErrorAlert error={error} />

            <Flex justifyContent="end" mt={3}>
              <Button
                colorPalette="danger"
                loading={isDeleting}
                onClick={() => {
                  void bulkDelete(selectedRuns);
                }}
              >
                <FiTrash2 />{" "}
                <Text as="span" fontWeight="bold">
                  {translate("common:delete")}
                </Text>
              </Button>
            </Flex>
          </Dialog.Body>
        </Dialog.Content>
      </Dialog.Root>
    </>
  );
};

export default BulkDeleteRunsButton;
