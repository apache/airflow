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
import { Flex, useDisclosure, Text, VStack, Heading, Code } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiTrash2 } from "react-icons/fi";

import { Button, Dialog } from "src/components/ui";
import ActionButton from "src/components/ui/ActionButton";
import { useDeleteConnection } from "src/queries/useDeleteConnection";

type Props = {
  readonly connectionId: string;
  readonly disabled: boolean;
};

const DeleteConnectionButton = ({ connectionId, disabled }: Props) => {
  const { t: translate } = useTranslation("admin");
  const { onClose, onOpen, open } = useDisclosure();
  const { isPending, mutate } = useDeleteConnection({
    onSuccessConfirm: onClose,
  });

  return (
    <>
      <ActionButton
        actionName={translate("connections.delete.title")}
        colorPalette="danger"
        disabled={disabled}
        icon={<FiTrash2 />}
        onClick={() => {
          onOpen();
        }}
        text={translate("connections.delete.title")}
        withText={false}
      />

      <Dialog.Root onOpenChange={onClose} open={open} size="xl">
        <Dialog.Content backdrop>
          <Dialog.Header>
            <VStack align="start" gap={4}>
              <Heading size="xl">{translate("connections.delete.deleteConnection", { count: 1 })}</Heading>
            </VStack>
          </Dialog.Header>

          <Dialog.CloseTrigger />

          <Dialog.Body width="full">
            <Text color="fg" fontSize="md" fontWeight="semibold" mb={4}>
              {translate("connections.delete.firstConfirmMessage_one")}
              <br />
              <Code mb={2} mt={2} p={4}>
                {connectionId}
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
                    connectionId,
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

export default DeleteConnectionButton;
