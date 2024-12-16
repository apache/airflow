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
import { Heading, VStack } from "@chakra-ui/react";
import React from "react";

import { Alert, Dialog } from "src/components/ui";

import { TogglePause } from "../TogglePause";
import TriggerDAGForm from "./TriggerDAGForm";

type TriggerDAGModalProps = {
  dagDisplayName: string;
  dagId: string;
  isPaused: boolean;
  onClose: () => void;
  open: boolean;
};

const TriggerDAGModal: React.FC<TriggerDAGModalProps> = ({
  dagDisplayName,
  dagId,
  isPaused,
  onClose,
  open,
}) => (
  <Dialog.Root
    lazyMount
    onOpenChange={onClose}
    open={open}
    size="xl"
    unmountOnExit
  >
    <Dialog.Content backdrop>
      <Dialog.Header>
        <VStack align="start" gap={4}>
          <Heading size="xl">
            Trigger DAG - {dagDisplayName}{" "}
            <TogglePause dagId={dagId} isPaused={isPaused} skipConfirm />
          </Heading>
          {isPaused ? (
            <Alert status="warning" title="Paused DAG">
              Triggering will create a DAG run, but it will not start until the
              DAG is unpaused.
            </Alert>
          ) : undefined}
        </VStack>
      </Dialog.Header>

      <Dialog.CloseTrigger />

      <Dialog.Body>
        <TriggerDAGForm dagId={dagId} onClose={onClose} open={open} />
      </Dialog.Body>
    </Dialog.Content>
  </Dialog.Root>
);

export default TriggerDAGModal;
