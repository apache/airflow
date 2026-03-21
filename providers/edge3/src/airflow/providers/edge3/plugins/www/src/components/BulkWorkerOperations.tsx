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
import { Button, Dialog, HStack, List, Portal, Text, useDisclosure } from "@chakra-ui/react";
import type { Worker } from "openapi/requests/types.gen";
import { FaPowerOff } from "react-icons/fa";
import { FaRegTrashCan } from "react-icons/fa6";

import { useBulkWorkerActions } from "src/hooks/useBulkWorkerActions";

type BulkWorkerOperationsProps = {
  readonly onClearSelection: VoidFunction;
  readonly onOperations: () => void;
  readonly selectedWorkers: Array<Worker>;
};

export const BulkWorkerOperations = ({
  onClearSelection,
  onOperations,
  selectedWorkers,
}: BulkWorkerOperationsProps) => {
  const {
    onClose: onCloseShutdownDialog,
    onOpen: onOpenShutdownDialog,
    open: isShutdownDialogOpen,
  } = useDisclosure();
  const {
    onClose: onCloseDeleteDialog,
    onOpen: onOpenDeleteDialog,
    open: isDeleteDialogOpen,
  } = useDisclosure();
  const {
    deleteWorkers,
    handleBulkDelete,
    handleBulkShutdown,
    isBulkDeletePending,
    isBulkShutdownPending,
    shutdownWorkers,
  } = useBulkWorkerActions({
    onClearSelection,
    onOperations,
    selectedWorkers,
  });

  const onBulkShutdown = async () => {
    try {
      await handleBulkShutdown();
    } finally {
      onCloseShutdownDialog();
    }
  };

  const onBulkDelete = async () => {
    try {
      await handleBulkDelete();
    } finally {
      onCloseDeleteDialog();
    }
  };

  return (
    <>
      <HStack>
        <Button
          colorPalette="danger"
          disabled={shutdownWorkers.length === 0}
          onClick={onOpenShutdownDialog}
          size="sm"
          variant="outline"
        >
          <FaPowerOff />
          Shutdown ({shutdownWorkers.length})
        </Button>
        <Button
          colorPalette="danger"
          disabled={deleteWorkers.length === 0}
          onClick={onOpenDeleteDialog}
          size="sm"
          variant="outline"
        >
          <FaRegTrashCan />
          Delete ({deleteWorkers.length})
        </Button>
      </HStack>

      <Dialog.Root onOpenChange={onCloseShutdownDialog} open={isShutdownDialogOpen} size="lg">
        <Portal>
          <Dialog.Backdrop />
          <Dialog.Positioner>
            <Dialog.Content>
              <Dialog.Header>
                <Dialog.Title>
                  Shutdown {shutdownWorkers.length} selected worker(s)
                </Dialog.Title>
              </Dialog.Header>
              <Dialog.Body>
                <Text mb={3}>
                  Shutdown can be requested only for workers in states: idle, running, maintenance pending,
                  maintenance mode, or maintenance request.
                </Text>
                <List.Root ps={5}>
                  {shutdownWorkers.map((worker) => (
                    <List.Item key={worker.worker_name}>{worker.worker_name}</List.Item>
                  ))}
                </List.Root>
              </Dialog.Body>
              <Dialog.Footer>
                <Dialog.ActionTrigger asChild>
                  <Button variant="outline">Cancel</Button>
                </Dialog.ActionTrigger>
                <Button
                  colorPalette="danger"
                  loading={isBulkShutdownPending}
                  loadingText="Shutting down..."
                  onClick={onBulkShutdown}
                >
                  <FaPowerOff />
                  Request Shutdown
                </Button>
              </Dialog.Footer>
            </Dialog.Content>
          </Dialog.Positioner>
        </Portal>
      </Dialog.Root>

      <Dialog.Root onOpenChange={onCloseDeleteDialog} open={isDeleteDialogOpen} size="lg">
        <Portal>
          <Dialog.Backdrop />
          <Dialog.Positioner>
            <Dialog.Content>
              <Dialog.Header>
                <Dialog.Title>
                  Delete {deleteWorkers.length} selected worker(s)
                </Dialog.Title>
              </Dialog.Header>
              <Dialog.Body>
                <Text mb={3}>
                  Delete is available only for workers in states: offline, unknown, or offline maintenance.
                </Text>
                <List.Root ps={5}>
                  {deleteWorkers.map((worker) => (
                    <List.Item key={worker.worker_name}>{worker.worker_name}</List.Item>
                  ))}
                </List.Root>
              </Dialog.Body>
              <Dialog.Footer>
                <Dialog.ActionTrigger asChild>
                  <Button variant="outline">Cancel</Button>
                </Dialog.ActionTrigger>
                <Button
                  colorPalette="danger"
                  loading={isBulkDeletePending}
                  loadingText="Deleting..."
                  onClick={onBulkDelete}
                >
                  <FaRegTrashCan />
                  Delete Workers
                </Button>
              </Dialog.Footer>
            </Dialog.Content>
          </Dialog.Positioner>
        </Portal>
      </Dialog.Root>
    </>
  );
};
