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
import { Text, Heading, HStack, useDisclosure } from "@chakra-ui/react";
import { FiTrash2 } from "react-icons/fi";
import { useNavigate } from "react-router-dom";

import { Button, Dialog } from "src/components/ui";
import ActionButton from "src/components/ui/ActionButton";
import { useDeleteDag } from "src/queries/useDeleteDag";

type DeleteDagButtonProps = {
  readonly dagDisplayName: string;
  readonly dagId: string;
  readonly withText?: boolean;
};

const DeleteDagButton = ({ dagDisplayName, dagId, withText = true }: DeleteDagButtonProps) => {
  const { onClose, onOpen, open } = useDisclosure();
  const navigate = useNavigate();

  const { isPending, mutate: deleteDag } = useDeleteDag({
    dagId,
    onSuccessConfirm: () => {
      onClose();
      navigate("/dags");
    },
  });

  return (
    <>
      <ActionButton
        actionName="Delete DAG"
        colorPalette="red"
        icon={<FiTrash2 />}
        onClick={onOpen}
        text="Delete DAG"
        variant="solid"
        withText={withText}
      />

      <Dialog.Root lazyMount onOpenChange={onClose} open={open} size="md" unmountOnExit>
        <Dialog.Content backdrop>
          <Dialog.Header>
            <Heading size="lg">Delete DAG</Heading>
          </Dialog.Header>
          <Dialog.CloseTrigger />
          <Dialog.Body>
            <Text>
              Are you sure you want to delete <strong>{dagDisplayName}</strong>? This action cannot be undone.
            </Text>
            <Text color="red.500" fontWeight="bold" mt={4}>
              This will remove all metadata related to the DAG, including DAG Runs and Tasks.
            </Text>
          </Dialog.Body>
          <Dialog.Footer>
            <HStack justifyContent="flex-end" width="100%">
              <Button onClick={onClose} variant="outline">
                Cancel
              </Button>
              <Button colorPalette="red" loading={isPending} onClick={() => deleteDag({ dagId })}>
                <FiTrash2 style={{ marginRight: "8px" }} /> Delete
              </Button>
            </HStack>
          </Dialog.Footer>
        </Dialog.Content>
      </Dialog.Root>
    </>
  );
};

export default DeleteDagButton;
