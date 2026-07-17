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
import { Button, CloseButton, Dialog, IconButton, Portal, Text, useDisclosure } from "@chakra-ui/react";
import { useUiServiceDeleteWorker } from "openapi/queries";
import { FaRegTrashCan } from "react-icons/fa6";

interface WorkerDeleteButtonProps {
  onDelete: (toast: Record<string, string>) => void;
  workerName: string;
}

export const WorkerDeleteButton = ({ onDelete, workerName }: WorkerDeleteButtonProps) => {
  const { onClose, onOpen, open } = useDisclosure();

  const deleteMutation = useUiServiceDeleteWorker({
    onError: (error) => {
      onDelete({
        description: `Unable to delete worker ${workerName}: ${error}`,
        title: "Delete Worker Failed",
        type: "error",
      });
    },
    onSuccess: () => {
      onDelete({
        description: `Worker ${workerName} has been deleted from the system.`,
        title: "Worker Deleted",
        type: "success",
      });
      onClose();
    },
  });

  const handleDelete = () => {
    deleteMutation.mutate({ workerName });
  };

  return (
    <>
      <IconButton
        size="sm"
        variant="ghost"
        onClick={onOpen}
        aria-label="Delete Worker"
        title="Delete Worker"
        colorPalette="danger"
      >
        <FaRegTrashCan />
      </IconButton>

      <Dialog.Root onOpenChange={onClose} open={open} size="md">
        <Portal>
          <Dialog.Backdrop />
          <Dialog.Positioner>
            <Dialog.Content>
              <Dialog.Header>
                <Dialog.Title>Delete worker {workerName}</Dialog.Title>
              </Dialog.Header>
              <Dialog.Body>
                <Text>Are you sure you want to delete worker {workerName}?</Text>
                <Text fontSize="sm" color="red.500" mt={2}>
                  This will permanently remove the worker record from the system. This action cannot be
                  undone.
                </Text>
              </Dialog.Body>
              <Dialog.Footer>
                <Dialog.ActionTrigger asChild>
                  <Button variant="outline">Cancel</Button>
                </Dialog.ActionTrigger>
                <Button
                  onClick={handleDelete}
                  colorPalette="danger"
                  loading={deleteMutation.isPending}
                  loadingText="Deleting..."
                >
                  <FaRegTrashCan style={{ marginRight: "8px" }} />
                  Delete Worker
                </Button>
              </Dialog.Footer>
              <Dialog.CloseTrigger asChild>
                <CloseButton size="sm" />
              </Dialog.CloseTrigger>
            </Dialog.Content>
          </Dialog.Positioner>
        </Portal>
      </Dialog.Root>
    </>
  );
};
