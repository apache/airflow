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
import { Box, CloseButton, Dialog, Heading, useDisclosure, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiPlusCircle } from "react-icons/fi";

import ActionButton from "src/components/ui/ActionButton";
import { useAddConnection } from "src/queries/useAddConnection";

import { ConnectionForm } from "./ConnectionForm";
import type { ConnectionBody } from "./Connections";

const AddConnectionButton = () => {
  const { t: translate } = useTranslation("admin");
  const { onClose, onOpen, open } = useDisclosure();
  const { addConnection, error, isPending } = useAddConnection({ onSuccessConfirm: onClose });
  const initialConnection: ConnectionBody = {
    conn_type: "",
    connection_id: "",
    description: "",
    extra: "{}",
    host: "",
    login: "",
    password: "",
    port: "",
    schema: "",
  };

  return (
    <Box>
      <ActionButton
        actionName={translate("connections.add")}
        colorPalette="brand"
        icon={<FiPlusCircle />}
        onClick={onOpen}
        text={translate("connections.add")}
        variant="solid"
      />

      <Dialog.Root lazyMount onOpenChange={onClose} open={open} size="xl" unmountOnExit>
        <Dialog.Backdrop />
        <Dialog.Positioner>
          <Dialog.Content>
            <Dialog.Header paddingBottom={0}>
              <VStack align="start" gap={4}>
                <Heading size="xl">{translate("connections.add")}</Heading>
              </VStack>
            </Dialog.Header>

            <Dialog.CloseTrigger asChild position="absolute" right="2" top="2">
              <CloseButton size="sm" />
            </Dialog.CloseTrigger>

            <Dialog.Body>
              <ConnectionForm
                error={error}
                initialConnection={initialConnection}
                isPending={isPending}
                mutateConnection={addConnection}
              />
            </Dialog.Body>
          </Dialog.Content>
        </Dialog.Positioner>
      </Dialog.Root>
    </Box>
  );
};

export default AddConnectionButton;
