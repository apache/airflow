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
import { Button, Code, Flex, Heading, Text, useDisclosure, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiTrash2 } from "react-icons/fi";

import { ErrorAlert } from "src/components/ErrorAlert";
import { Dialog } from "src/components/ui";
import { useBulkDeleteVariables } from "src/queries/useBulkDeleteVariables";

type Props = {
  readonly clearSelections: VoidFunction;
  readonly deleteKeys: Array<string>;
};

const DeleteVariablesButton = ({ clearSelections, deleteKeys: variableKeys }: Props) => {
  const { t: translate } = useTranslation("admin");
  const { onClose, onOpen, open } = useDisclosure();
  const { error, isPending, mutate } = useBulkDeleteVariables({ clearSelections, onSuccessConfirm: onClose });

  return (
    <>
      <Button
        colorPalette="danger"
        onClick={() => {
          onOpen();
        }}
        size="sm"
        variant="outline"
      >
        <FiTrash2 />
        {translate("deleteActions.button")}
      </Button>

      <Dialog.Root onOpenChange={onClose} open={open} size="xl">
        <Dialog.Content backdrop>
          <Dialog.Header>
            <VStack align="start" gap={4}>
              <Heading size="xl">
                {translate("variables.delete.deleteVariable", {
                  count: variableKeys.length,
                })}
              </Heading>
            </VStack>
          </Dialog.Header>

          <Dialog.CloseTrigger />
          <Dialog.Body width="full">
            <Text color="fg" fontSize="md" fontWeight="semibold" mb={4}>
              {translate("variables.delete.firstConfirmMessage", { count: variableKeys.length })}
              <br />
              <Code mb={2} mt={2} p={4}>
                {variableKeys.join(", ")}
              </Code>
              <br />
              {translate("deleteActions.modal.secondConfirmMessage")}
              <strong>{translate("deleteActions.modal.thirdConfirmMessage")}</strong>
            </Text>
            <ErrorAlert error={error} />
            <Flex justifyContent="end" mt={3}>
              <Button
                colorPalette="danger"
                loading={isPending}
                onClick={() => {
                  mutate({
                    requestBody: {
                      actions: [
                        {
                          action: "delete" as const,
                          action_on_non_existence: "fail",
                          entities: variableKeys,
                        },
                      ],
                    },
                  });
                }}
              >
                <FiTrash2 /> <Text fontWeight="bold">{translate("deleteActions.modal.confirmButton")}</Text>
              </Button>
            </Flex>
          </Dialog.Body>
        </Dialog.Content>
      </Dialog.Root>
    </>
  );
};

export default DeleteVariablesButton;
