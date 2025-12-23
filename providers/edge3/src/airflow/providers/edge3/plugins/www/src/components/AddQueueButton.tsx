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
  Input,
  IconButton,
  Portal,
  Text,
  useDisclosure,
  VStack,
} from "@chakra-ui/react";
import { useUiServiceAddWorkerQueue } from "openapi/queries";
import { useState } from "react";
import { LuListPlus } from "react-icons/lu";

interface AddQueueButtonProps {
  onQueueUpdate: (toast: Record<string, string>) => void;
  workerName: string;
}

export const AddQueueButton = ({ onQueueUpdate, workerName }: AddQueueButtonProps) => {
  const { onClose, onOpen, open } = useDisclosure();
  const [queueName, setQueueName] = useState("");

  const addQueueMutation = useUiServiceAddWorkerQueue({
    onError: (error) => {
      onQueueUpdate({
        description: `Unable to add queue to worker ${workerName}: ${error}`,
        title: "Add Queue Failed",
        type: "error",
      });
    },
    onSuccess: () => {
      onQueueUpdate({
        description: `Queue "${queueName}" was added to worker ${workerName}.`,
        title: "Queue Added",
        type: "success",
      });
      onClose();
      setQueueName("");
    },
  });

  const handleAddQueue = () => {
    if (!queueName.trim()) {
      onQueueUpdate({
        description: "Please enter a queue name.",
        title: "Invalid Input",
        type: "error",
      });
      return;
    }

    addQueueMutation.mutate({
      queueName: queueName.trim(),
      workerName,
    });
  };

  return (
    <>
      <IconButton
        size="sm"
        variant="ghost"
        onClick={onOpen}
        aria-label="Add Queue"
        title="Add Queue"
        colorPalette="success"
      >
        <LuListPlus />
      </IconButton>

      <Dialog.Root onOpenChange={onClose} open={open} size="md">
        <Portal>
          <Dialog.Backdrop />
          <Dialog.Positioner>
            <Dialog.Content>
              <Dialog.Header>
                <Dialog.Title>Add Queue to {workerName}</Dialog.Title>
              </Dialog.Header>
              <Dialog.Body>
                <VStack gap={4} align="stretch">
                  <Text>Enter the name of the queue to add to this worker:</Text>
                  <Input
                    placeholder="Queue name"
                    value={queueName}
                    onChange={(e) => setQueueName(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") {
                        handleAddQueue();
                      }
                    }}
                  />
                </VStack>
              </Dialog.Body>
              <Dialog.Footer>
                <Dialog.ActionTrigger asChild>
                  <Button variant="outline">Cancel</Button>
                </Dialog.ActionTrigger>
                <Button
                  onClick={handleAddQueue}
                  colorPalette="success"
                  loading={addQueueMutation.isPending}
                  loadingText="Adding queue..."
                  disabled={!queueName.trim()}
                >
                  <LuListPlus />
                  Add Queue
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
