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
import { Box, Heading, Spinner, VStack } from "@chakra-ui/react";
import { useDisclosure } from "@chakra-ui/react";
import { FiPlusCircle } from "react-icons/fi";

import { Dialog } from "src/components/ui";
import ActionButton from "src/components/ui/ActionButton";
import { useConnectionTypeMeta } from "src/queries/useConnectionTypeMeta";

import ConnectionForm from "./ConnectionForm";

export type AddConnectionParams = {
  conf: string;
  conn_type: string;
  connection_id: string;
  description: string;
  host: string;
  login: string;
  password: string;
  port: string;
  schema: string;
};

const AddConnectionButton = () => {
  const { onClose, onOpen, open } = useDisclosure();
  const { formattedData: connectionTypeMeta, isPending, keysList: connectionTypes } = useConnectionTypeMeta();

  return (
    <Box>
      <ActionButton
        actionName="Add Connection"
        colorPalette="blue"
        disabled={isPending}
        icon={isPending ? <Spinner size="sm" /> : <FiPlusCircle />}
        onClick={onOpen}
        text="Add Connection"
        variant="solid"
      />

      <Dialog.Root lazyMount onOpenChange={onClose} open={open} size="xl" unmountOnExit>
        <Dialog.Content backdrop>
          <Dialog.Header paddingBottom={0}>
            <VStack align="start" gap={4}>
              <Heading size="xl">Add Connection</Heading>
            </VStack>
          </Dialog.Header>

          <Dialog.CloseTrigger />

          <Dialog.Body>
            <ConnectionForm
              connectionTypeMeta={connectionTypeMeta}
              connectionTypes={connectionTypes}
              onClose={onClose}
            />
          </Dialog.Body>
        </Dialog.Content>
      </Dialog.Root>
    </Box>
  );
};

export default AddConnectionButton;
