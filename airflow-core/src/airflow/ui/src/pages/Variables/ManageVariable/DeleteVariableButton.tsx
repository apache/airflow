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
import { CloseButton, Code, Dialog, Flex, Heading, Text, useDisclosure, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiTrash2 } from "react-icons/fi";

import { Button } from "src/components/ui";
import ActionButton from "src/components/ui/ActionButton";
import { useDeleteVariable } from "src/queries/useDeleteVariable";

type Props = {
  readonly deleteKey: string;
  readonly disabled: boolean;
};

const DeleteVariableButton = ({ deleteKey: variableKey, disabled }: Props) => {
  const { t: translate } = useTranslation("admin");
  const { onClose, onOpen, open } = useDisclosure();
  const { isPending, mutate } = useDeleteVariable({
    onSuccessConfirm: onClose,
  });

  return (
    <>
      <ActionButton
        actionName={translate("variables.delete.title")}
        colorPalette="danger"
        disabled={disabled}
        icon={<FiTrash2 />}
        onClick={() => {
          onOpen();
        }}
        text={translate("variables.delete.title")}
        withText={false}
      />

      <Dialog.Root onOpenChange={onClose} open={open} size="xl">
        <Dialog.Backdrop />
        <Dialog.Positioner>
          <Dialog.Content>
            <Dialog.Header>
              <VStack align="start" gap={4}>
                <Heading size="xl">{translate("variables.delete.deleteVariable", { count: 1 })}</Heading>
              </VStack>
            </Dialog.Header>

            <Dialog.CloseTrigger asChild position="absolute" right="2" top="2">
              <CloseButton size="sm" />
            </Dialog.CloseTrigger>

            <Dialog.Body width="full">
              <Text color="fg" fontSize="md" fontWeight="semibold" mb={4}>
                {translate("variables.delete.firstConfirmMessage_one")}
                <br />
                <Code mb={2} mt={2} p={4}>
                  {variableKey}
                </Code>
                <br />
                {translate("deleteActions.modal.secondConfirmMessage")}
                <strong>{translate("deleteActions.modal.thirdConfirmMessage")}</strong>
              </Text>
              <Flex justifyContent="end" mt={3}>
                <Button
                  colorPalette="danger"
                  loading={isPending}
                  onClick={() => {
                    mutate({
                      variableKey,
                    });
                  }}
                >
                  <FiTrash2 /> <Text fontWeight="bold">{translate("deleteActions.modal.confirmButton")}</Text>
                </Button>
              </Flex>
            </Dialog.Body>
          </Dialog.Content>
        </Dialog.Positioner>
      </Dialog.Root>
    </>
  );
};

export default DeleteVariableButton;
