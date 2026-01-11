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
import { useUiServiceRequestWorkerShutdown } from "openapi/queries";
import { FaPowerOff } from "react-icons/fa";

interface WorkerShutdownButtonProps {
  onShutdown: (toast: Record<string, string>) => void;
  workerName: string;
}

export const WorkerShutdownButton = ({ onShutdown, workerName }: WorkerShutdownButtonProps) => {
  const { onClose, onOpen, open } = useDisclosure();

  const shutdownMutation = useUiServiceRequestWorkerShutdown({
    onError: (error) => {
      onShutdown({
        description: `Unable to request shutdown for worker ${workerName}: ${error}`,
        title: "Shutdown Request Failed",
        type: "error",
      });
    },
    onSuccess: () => {
      onShutdown({
        description: `Worker ${workerName} was requested to shutdown.`,
        title: "Shutdown Request Sent",
        type: "success",
      });
      onClose();
    },
  });

  const handleShutdown = () => {
    shutdownMutation.mutate({ workerName });
  };

  return (
    <>
      <IconButton
        size="sm"
        variant="ghost"
        onClick={onOpen}
        aria-label="Shutdown Worker"
        title="Shutdown Worker"
        colorPalette="danger"
      >
        <FaPowerOff />
      </IconButton>

      <Dialog.Root onOpenChange={onClose} open={open} size="md">
        <Portal>
          <Dialog.Backdrop />
          <Dialog.Positioner>
            <Dialog.Content>
              <Dialog.Header>
                <Dialog.Title>Shutdown worker {workerName}</Dialog.Title>
              </Dialog.Header>
              <Dialog.Body>
                <Text>Are you sure you want to request shutdown for worker {workerName}?</Text>
                <Text fontSize="sm" color="red.500" mt={2}>
                  This stops the worker on the remote edge. You can't restart it from the UI — you need to
                  start it remotely instead.
                </Text>
              </Dialog.Body>
              <Dialog.Footer>
                <Dialog.ActionTrigger asChild>
                  <Button variant="outline">Cancel</Button>
                </Dialog.ActionTrigger>
                <Button
                  onClick={handleShutdown}
                  colorPalette="danger"
                  loading={shutdownMutation.isPending}
                  loadingText="Shutting down..."
                >
                  <FaPowerOff style={{ marginRight: "8px" }} />
                  Shutdown Worker
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
