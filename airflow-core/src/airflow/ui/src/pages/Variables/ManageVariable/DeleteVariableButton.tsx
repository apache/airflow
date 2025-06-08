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
import { Trans, useTranslation } from "react-i18next";
import { FiTrash } from "react-icons/fi";

import { Button, Dialog } from "src/components/ui";
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
        actionName={translate("variables.delete")}
        disabled={disabled}
        icon={<FiTrash />}
        onClick={() => {
          onOpen();
        }}
        text={translate("variables.delete")}
        withText={false}
      />

      <Dialog.Root onOpenChange={onClose} open={open} size="xl">
        <Dialog.Content backdrop>
          <Dialog.Header>
            <VStack align="start" gap={4}>
              <Heading size="xl">{translate("variables.delete.deleteVariable", { count: 1 })}</Heading>
            </VStack>
          </Dialog.Header>

          <Dialog.CloseTrigger />

          <Dialog.Body width="full">
            <Text color="gray.solid" fontSize="md" fontWeight="semibold" mb={4}>
              <Trans count={1} i18nKey="admin:variables.delete.firstConfirmMessage" />
              <br />
              <Code mb={2} mt={2} p={4}>
                {variableKey}
              </Code>
              <br />
              <Trans i18nKey="admin:variables.delete.secondConfirmMessage" />
            </Text>
            <Flex justifyContent="end" mt={3}>
              <Button
                colorPalette="red"
                loading={isPending}
                onClick={() => {
                  mutate({
                    variableKey,
                  });
                }}
              >
                <FiTrash /> <Text fontWeight="bold">{translate("variables.delete.confirmButton")}</Text>
              </Button>
            </Flex>
          </Dialog.Body>
        </Dialog.Content>
      </Dialog.Root>
    </>
  );
};

export default DeleteVariableButton;
