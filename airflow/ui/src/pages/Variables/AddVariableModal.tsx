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
import { Heading, useDisclosure } from "@chakra-ui/react";
import { FiPlusCircle } from "react-icons/fi";

import { Button, Dialog, Toaster } from "src/components/ui";

import AddVariableForm from "./AddVariableForm";

const AddVariableModal: React.FC = () => {
  const { onClose, onOpen, open } = useDisclosure();

  return (
    <>
      <Toaster />
      <Button colorPalette="blue" onClick={onOpen}>
        <FiPlusCircle /> Add Variable
      </Button>
      <Dialog.Root onOpenChange={onClose} open={open} size="xl">
        <Dialog.Content backdrop>
          <Dialog.Header>
            <Heading size="xl">Add Variable</Heading>
          </Dialog.Header>

          <Dialog.CloseTrigger />

          <Dialog.Body>
            <AddVariableForm onClose={onClose} />
          </Dialog.Body>
        </Dialog.Content>
      </Dialog.Root>
    </>
  );
};

export default AddVariableModal;
