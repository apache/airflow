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
import { Heading, useDisclosure, VStack } from "@chakra-ui/react";
import { FiUploadCloud } from "react-icons/fi";

import { Button, Dialog } from "src/components/ui";

import ImportVariablesForm from "./ImportVariablesForm";

type Props = {
  readonly disabled: boolean;
};

const ImportVariablesButton = ({ disabled }: Props) => {
  const { onClose, onOpen, open } = useDisclosure();

  return (
    <>
      <Button colorPalette="blue" disabled={disabled} onClick={onOpen}>
        <FiUploadCloud /> Import Variables
      </Button>

      <Dialog.Root onOpenChange={onClose} open={open} size="xl">
        <Dialog.Content backdrop>
          <Dialog.Header>
            <VStack align="start" gap={4}>
              <Heading size="xl"> Import Variables </Heading>
            </VStack>
          </Dialog.Header>

          <Dialog.CloseTrigger />

          <Dialog.Body width="full">
            <ImportVariablesForm onClose={onClose} />
          </Dialog.Body>
        </Dialog.Content>
      </Dialog.Root>
    </>
  );
};

export default ImportVariablesButton;
