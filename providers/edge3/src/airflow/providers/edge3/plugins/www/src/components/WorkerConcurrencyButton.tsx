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
  Input,
  Portal,
  Text,
  VStack,
  useDisclosure,
} from "@chakra-ui/react";
import { useUiServiceSetWorkerConcurrencyLimit } from "openapi/queries";
import type { Worker } from "openapi/requests/types.gen";
import { useState } from "react";
import { LuSlidersHorizontal } from "react-icons/lu";

interface WorkerConcurrencyButtonProps {
  onConcurrencyUpdate: (toast: Record<string, string>) => void;
  worker: Worker;
}

export const WorkerConcurrencyButton = ({
  onConcurrencyUpdate,
  worker,
}: WorkerConcurrencyButtonProps) => {
  const { onClose, onOpen, open } = useDisclosure();
  const workerName = worker.worker_name;
  const currentConcurrency = worker.sysinfo?.concurrency;
  const [concurrency, setConcurrency] = useState<string>(
    currentConcurrency !== undefined ? String(currentConcurrency) : "",
  );

  const setConcurrencyMutation = useUiServiceSetWorkerConcurrencyLimit({
    onError: (error: unknown) => {
      onConcurrencyUpdate({
        description: `Unable to set concurrency for worker ${workerName}: ${error}`,
        title: "Set Concurrency Failed",
        type: "error",
      });
    },
    onSuccess: () => {
      onConcurrencyUpdate({
        description: `Concurrency for worker ${workerName} set to ${concurrency}.`,
        title: "Concurrency Updated",
        type: "success",
      });
      onClose();
    },
  });

  const handleSetConcurrency = () => {
    const value = parseInt(concurrency, 10);

    if (!concurrency.trim() || isNaN(value) || value <= 0) {
      onConcurrencyUpdate({
        description: "Please enter a valid concurrency value greater than 0.",
        title: "Invalid Input",
        type: "error",
      });
      return;
    }

    setConcurrencyMutation.mutate({
      requestBody: { concurrency: value },
      workerName,
    });
  };

  const handleOpen = () => {
    setConcurrency(currentConcurrency !== undefined ? String(currentConcurrency) : "");
    onOpen();
  };

  const concurrencyValue = parseInt(concurrency, 10);
  const isValid = concurrency.trim() !== "" && !isNaN(concurrencyValue) && concurrencyValue > 0;

  return (
    <>
      <IconButton
        aria-label="Set Concurrency"
        colorPalette="blue"
        onClick={handleOpen}
        size="sm"
        title="Set Concurrency"
        variant="ghost"
      >
        <LuSlidersHorizontal />
      </IconButton>

      <Dialog.Root onOpenChange={onClose} open={open} size="md">
        <Portal>
          <Dialog.Backdrop />
          <Dialog.Positioner>
            <Dialog.Content>
              <Dialog.Header>
                <Dialog.Title>Set Concurrency for {workerName}</Dialog.Title>
              </Dialog.Header>
              <Dialog.Body>
                <VStack align="stretch" gap={4}>
                  <Text>Enter the new concurrency limit for this worker:</Text>
                  <Input
                    min={1}
                    onChange={(e) => setConcurrency(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") {
                        handleSetConcurrency();
                      }
                    }}
                    placeholder="Concurrency limit"
                    type="number"
                    value={concurrency}
                  />
                </VStack>
              </Dialog.Body>
              <Dialog.Footer>
                <Dialog.ActionTrigger asChild>
                  <Button variant="outline">Cancel</Button>
                </Dialog.ActionTrigger>
                <Button
                  colorPalette="blue"
                  disabled={!isValid}
                  loading={setConcurrencyMutation.isPending}
                  loadingText="Setting concurrency..."
                  onClick={handleSetConcurrency}
                >
                  <LuSlidersHorizontal />
                  Set Concurrency
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
