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

import { Dialog } from "src/components/ui";

import TriggerDAGForm from "./TriggerDAGForm";

type TriggerDAGModalProps = {
  readonly dagDisplayName: string;
  readonly dagId: string;
  readonly isPaused: boolean;
  readonly onClose: () => void;
  readonly open: boolean;
};

const TriggerDAGModal: React.FC<TriggerDAGModalProps> = ({
  dagDisplayName,
  dagId,
  isPaused,
  onClose,
  open,
}) => (
  <Dialog.Root lazyMount onOpenChange={onClose} open={open} size="xl" unmountOnExit>
    <Dialog.Content backdrop>
      <Dialog.Header paddingBottom={0}>
        <VStack align="start" gap={4}>
          <Heading size="xl">Trigger Dag - {dagDisplayName}</Heading>
        </VStack>
      </Dialog.Header>

      <Dialog.CloseTrigger />

      <Dialog.Body>
        <TriggerDAGForm dagId={dagId} isPaused={isPaused} onClose={onClose} open={open} />
      </Dialog.Body>
    </Dialog.Content>
  </Dialog.Root>
);

export default TriggerDAGModal;
