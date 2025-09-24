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
import { useUiServiceExitWorkerMaintenance } from "openapi/queries";
import { IoMdExit } from "react-icons/io";

interface MaintenanceExitButtonProps {
  onExitMaintenance: (toast: Record<string, string>) => void;
  workerName: string;
}

export const MaintenanceExitButton = ({ onExitMaintenance, workerName }: MaintenanceExitButtonProps) => {
  const { onClose, onOpen, open } = useDisclosure();

  const exitMaintenanceMutation = useUiServiceExitWorkerMaintenance({
    onError: (error) => {
      onExitMaintenance({
        description: `Unable to exit ${workerName} from maintenance mode: ${error}`,
        title: "Exit Maintenance Mode failed",
        type: "error",
      });
    },
    onSuccess: () => {
      onExitMaintenance({
        description: `Worker ${workerName} was requested to exit maintenance mode.`,
        title: "Maintenance Mode deactivated",
        type: "success",
      });
      onClose();
    },
  });

  const exitMaintenance = () => {
    exitMaintenanceMutation.mutate({ workerName });
  };

  return (
    <>
      <IconButton
        size="sm"
        variant="ghost"
        onClick={onOpen}
        aria-label="Exit Maintenance"
        title="Exit Maintenance"
        colorPalette="warning"
      >
        <IoMdExit />
      </IconButton>

      <Dialog.Root onOpenChange={onClose} open={open} size="md">
        <Portal>
          <Dialog.Backdrop />
          <Dialog.Positioner>
            <Dialog.Content>
              <Dialog.Header>
                <Dialog.Title>Exit maintenance for worker {workerName}</Dialog.Title>
              </Dialog.Header>
              <Dialog.Body>
                <Text>Are you sure you want to exit maintenance mode for worker {workerName}?</Text>
              </Dialog.Body>
              <Dialog.Footer>
                <Dialog.ActionTrigger asChild>
                  <Button variant="outline">No</Button>
                </Dialog.ActionTrigger>
                <Button onClick={exitMaintenance}>Yes</Button>
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
