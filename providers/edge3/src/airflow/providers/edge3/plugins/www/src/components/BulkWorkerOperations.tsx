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
import { useUiServiceDeleteWorker, useUiServiceRequestWorkerShutdown } from "openapi/queries";
import type { Worker } from "openapi/requests/types.gen";
import { useMemo, useState } from "react";
import { FaPowerOff } from "react-icons/fa";
import { FaRegTrashCan } from "react-icons/fa6";

import { toaster } from "src/components/ui";

const shutdownEligibleStates = new Set([
  "idle",
  "running",
  "maintenance pending",
  "maintenance mode",
  "maintenance request",
]);
const deleteEligibleStates = new Set(["offline", "unknown", "offline maintenance"]);

type BulkWorkerOperationsProps = {
  readonly onClearSelection: VoidFunction;
  readonly onOperations: () => void;
  readonly selectedWorkers: Array<Worker>;
};

type BulkActionResult = {
  failedWorkers: Array<string>;
  successCount: number;
};

const getFailureDescription = (
  failedWorkers: Array<string>,
): string => {
  const maxFailuresToDisplay = 5;
  const displayedFailedWorkers = failedWorkers.slice(0, maxFailuresToDisplay);
  const additionalFailedWorkersCount = failedWorkers.length - displayedFailedWorkers.length;
  const additionalFailuresMessage =
    additionalFailedWorkersCount > 0 ? ` and ${additionalFailedWorkersCount} more` : "";

  return `Failed for ${failedWorkers.length} worker(s): ${displayedFailedWorkers.join(", ")}${additionalFailuresMessage}.`;
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
  const [isBulkDeletePending, setIsBulkDeletePending] = useState(false);
  const [isBulkShutdownPending, setIsBulkShutdownPending] = useState(false);

  const shutdownMutation = useUiServiceRequestWorkerShutdown();
  const deleteMutation = useUiServiceDeleteWorker();

  const shutdownWorkers = useMemo(
    () => selectedWorkers.filter((worker) => shutdownEligibleStates.has(worker.state)),
    [selectedWorkers],
  );
  const deleteWorkers = useMemo(
    () => selectedWorkers.filter((worker) => deleteEligibleStates.has(worker.state)),
    [selectedWorkers],
  );

  const handleBulkShutdown = async () => {
    setIsBulkShutdownPending(true);

    const results = await Promise.allSettled(
      shutdownWorkers.map((worker) => shutdownMutation.mutateAsync({ workerName: worker.worker_name })),
    );

    const failedWorkers = results.flatMap((result, index) =>
      result.status === "rejected" ? [shutdownWorkers[index]?.worker_name ?? "unknown"] : [],
    );
    const successCount = shutdownWorkers.length - failedWorkers.length;

    const actionResult: BulkActionResult = {
      failedWorkers,
      successCount,
    };

    if (actionResult.successCount > 0) {
      toaster.create({
        description: `Shutdown requested for ${actionResult.successCount} worker(s).`,
        title: "Bulk Shutdown Requested",
        type: "success",
      });
    }

    if (actionResult.failedWorkers.length > 0) {
      toaster.create({
        description: getFailureDescription(actionResult.failedWorkers),
        title: "Bulk Shutdown Partially Failed",
        type: "error",
      });
    }

    if (actionResult.successCount > 0) {
      onOperations();
      onClearSelection();
    }

    onCloseShutdownDialog();
    setIsBulkShutdownPending(false);
  };

  const handleBulkDelete = async () => {
    setIsBulkDeletePending(true);

    const results = await Promise.allSettled(
      deleteWorkers.map((worker) => deleteMutation.mutateAsync({ workerName: worker.worker_name })),
    );

    const failedWorkers = results.flatMap((result, index) =>
      result.status === "rejected" ? [deleteWorkers[index]?.worker_name ?? "unknown"] : [],
    );
    const successCount = deleteWorkers.length - failedWorkers.length;

    const actionResult: BulkActionResult = {
      failedWorkers,
      successCount,
    };

    if (actionResult.successCount > 0) {
      toaster.create({
        description: `${actionResult.successCount} worker(s) deleted.`,
        title: "Bulk Delete Completed",
        type: "success",
      });
    }

    if (actionResult.failedWorkers.length > 0) {
      toaster.create({
        description: getFailureDescription(actionResult.failedWorkers),
        title: "Bulk Delete Partially Failed",
        type: "error",
      });
    }

    if (actionResult.successCount > 0) {
      onOperations();
      onClearSelection();
    }

    onCloseDeleteDialog();
    setIsBulkDeletePending(false);
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
                  onClick={handleBulkShutdown}
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
                  onClick={handleBulkDelete}
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
