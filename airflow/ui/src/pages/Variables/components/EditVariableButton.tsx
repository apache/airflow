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
import { Box, Heading, useDisclosure } from "@chakra-ui/react";
import { FiEdit } from "react-icons/fi";
import { Dialog } from "src/components/ui";

import ActionButton from "src/components/ui/ActionButton";
import VariableForm from "./VariableForm";
import type { VariableResponse } from "openapi/requests/types.gen";

type Props = {
  readonly variable: VariableResponse;
};

const EditVariableButton = ({ variable: initialVariable }: Props) => {
  const { onClose, onOpen, open } = useDisclosure();
  
  return(
  <Box>
    <ActionButton
      actionName="Edit Variable"
      icon={<FiEdit />}
      onClick={() => {
        onOpen();
      }}
      text="Edit Variable"
      withText={false}
    />
    <Dialog.Root onOpenChange={onClose} open={open} size="xl">
        <Dialog.Content backdrop>
          <Dialog.Header>
            <Heading size="xl">Edit Variable</Heading>
          </Dialog.Header>

          <Dialog.CloseTrigger />

          <Dialog.Body>
            <VariableForm onClose={onClose} />
          </Dialog.Body>
        </Dialog.Content>
      </Dialog.Root>
  </Box>
);
};

export default EditVariableButton;
