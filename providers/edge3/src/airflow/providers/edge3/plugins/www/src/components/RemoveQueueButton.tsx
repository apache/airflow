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
import {
  Button,
  CloseButton,
  Dialog,
  IconButton,
  Portal,
  Text,
  useDisclosure,
  VStack,
  For,
} from "@chakra-ui/react";
import { useUiServiceRemoveWorkerQueue } from "openapi/queries";
import type { Worker } from "openapi/requests/types.gen";
import { useState } from "react";
import { LuListMinus } from "react-icons/lu";

interface RemoveQueueButtonProps {
  onQueueUpdate: (toast: Record<string, string>) => void;
  worker: Worker;
}

export const RemoveQueueButton = ({ onQueueUpdate, worker }: RemoveQueueButtonProps) => {
  const { onClose, onOpen, open } = useDisclosure();
  const [selectedQueue, setSelectedQueue] = useState<string>("");

  const removeQueueMutation = useUiServiceRemoveWorkerQueue({
    onError: (error) => {
      onQueueUpdate({
        description: `Unable to remove queue from worker ${worker.worker_name}: ${error}`,
        title: "Remove Queue Failed",
        type: "error",
      });
    },
    onSuccess: () => {
      onQueueUpdate({
        description: `Queue "${selectedQueue}" was removed from worker ${worker.worker_name}.`,
        title: "Queue Removed",
        type: "success",
      });
      onClose();
      setSelectedQueue("");
    },
  });

  const handleRemoveQueue = () => {
    if (!selectedQueue) {
      onQueueUpdate({
        description: "Please select a queue to remove.",
        title: "Invalid Selection",
        type: "error",
      });
      return;
    }

    removeQueueMutation.mutate({
      queueName: selectedQueue,
      workerName: worker.worker_name,
    });
  };

  const availableQueues = worker.queues || [];

  // Don't render the button if there are no queues to remove
  if (availableQueues.length === 0) {
    return null;
  }

  return (
    <>
      <IconButton
        size="sm"
        variant="ghost"
        onClick={onOpen}
        aria-label="Remove Queue"
        title="Remove Queue"
        colorPalette="danger"
      >
        <LuListMinus />
      </IconButton>

      <Dialog.Root onOpenChange={onClose} open={open} size="md">
        <Portal>
          <Dialog.Backdrop />
          <Dialog.Positioner>
            <Dialog.Content>
              <Dialog.Header>
                <Dialog.Title>Remove Queue from {worker.worker_name}</Dialog.Title>
              </Dialog.Header>
              <Dialog.Body>
                <VStack gap={4} align="stretch">
                  <Text>Select a queue to remove from this worker:</Text>
                  <VStack gap={2} align="stretch">
                    <For each={availableQueues}>
                      {(queue) => (
                        <Button
                          key={queue}
                          variant={selectedQueue === queue ? "solid" : "outline"}
                          colorPalette={selectedQueue === queue ? "blue" : "gray"}
                          onClick={() => setSelectedQueue(queue)}
                          justifyContent="flex-start"
                        >
                          {queue}
                        </Button>
                      )}
                    </For>
                  </VStack>
                </VStack>
              </Dialog.Body>
              <Dialog.Footer>
                <Dialog.ActionTrigger asChild>
                  <Button variant="outline">Cancel</Button>
                </Dialog.ActionTrigger>
                <Button
                  onClick={handleRemoveQueue}
                  colorPalette="danger"
                  loading={removeQueueMutation.isPending}
                  loadingText="Removing queue..."
                  disabled={!selectedQueue}
                >
                  <LuListMinus />
                  Remove Queue
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
