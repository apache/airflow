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
import { Button, Heading, useDisclosure } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiPlusCircle } from "react-icons/fi";

import { Dialog, Toaster } from "src/components/ui";
import { useAddVariable } from "src/queries/useAddVariable";

import VariableForm, { type VariableBody } from "./VariableForm";

type Props = {
  readonly disabled: boolean;
};

const AddVariableButton = ({ disabled }: Props) => {
  const { t: translate } = useTranslation("admin");
  const { onClose, onOpen, open } = useDisclosure();
  const { addVariable, error, isPending, setError } = useAddVariable({
    onSuccessConfirm: onClose,
  });

  const initialVariableValue: VariableBody = {
    description: "",
    key: "",
    value: "",
  };

  const handleClose = () => {
    setError(undefined);
    onClose();
  };

  return (
    <>
      <Toaster />
      <Button colorPalette="brand" disabled={disabled} onClick={onOpen}>
        <FiPlusCircle /> {translate("variables.add")}
      </Button>

      <Dialog.Root onOpenChange={handleClose} open={open} size="xl">
        <Dialog.Content backdrop>
          <Dialog.Header>
            <Heading size="xl">{translate("variables.add")}</Heading>
          </Dialog.Header>

          <Dialog.CloseTrigger />

          <Dialog.Body>
            <VariableForm
              error={error}
              initialVariable={initialVariableValue}
              isPending={isPending}
              manageMutate={addVariable}
              setError={setError}
            />
          </Dialog.Body>
        </Dialog.Content>
      </Dialog.Root>
    </>
  );
};

export default AddVariableButton;
