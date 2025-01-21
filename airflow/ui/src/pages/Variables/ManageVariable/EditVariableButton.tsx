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
import { FiEdit } from "react-icons/fi";

import type { VariableResponse } from "openapi/requests/types.gen";
import { Dialog } from "src/components/ui";
import ActionButton from "src/components/ui/ActionButton";
import { useEditVariable } from "src/queries/useEditVariable";

import type { VariableBody } from "./VariableForm";
import VariableForm from "./VariableForm";

type Props = {
  readonly disabled: boolean;
  readonly variable: VariableResponse;
};

const EditVariableButton = ({ disabled, variable }: Props) => {
  const { onClose, onOpen, open } = useDisclosure();
  const initialVariableValue: VariableBody = {
    description: variable.description ?? "",
    key: variable.key,
    value: variable.value,
  };
  const { editVariable, error, isPending, setError } = useEditVariable(initialVariableValue, {
    onSuccessConfirm: onClose,
  });

  const handleClose = () => {
    setError(undefined);
    onClose();
  };

  return (
    <>
      <ActionButton
        actionName="Edit Variable"
        disabled={disabled}
        icon={<FiEdit />}
        onClick={() => {
          onOpen();
        }}
        text="Edit Variable"
        withText={false}
      />

      <Dialog.Root onOpenChange={handleClose} open={open} size="xl">
        <Dialog.Content backdrop>
          <Dialog.Header>
            <Heading size="xl">Edit Variable</Heading>
          </Dialog.Header>

          <Dialog.CloseTrigger />

          <Dialog.Body>
            <VariableForm
              error={error}
              initialVariable={initialVariableValue}
              isPending={isPending}
              manageMutate={editVariable}
              setError={setError}
            />
          </Dialog.Body>
        </Dialog.Content>
      </Dialog.Root>
    </>
  );
};

export default EditVariableButton;
