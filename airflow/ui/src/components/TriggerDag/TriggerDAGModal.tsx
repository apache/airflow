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
import { useDisclosure, Box, Text, Heading, VStack } from "@chakra-ui/react";
import React, { useState } from "react";
import { FiPlay } from "react-icons/fi";

import { Dialog } from "src/components/ui";

import TriggerDAGForm from "./TriggerDAGForm";
import type { DagParams } from "./TriggerDag";
import { TriggerDag } from "./TriggerDag";

type TriggerDAGModalProps = {
  dagDisplayName: string;
  dagId: string;
};

const TriggerDAGModal: React.FC<TriggerDAGModalProps> = ({
  dagDisplayName,
  dagId,
}) => {
  const { onClose, onOpen, open } = useDisclosure();
  const [dagParams, setDagParams] = useState<DagParams>({
    configJson: {},
    dagId,
    logicalDate: "",
    runId: "",
  });

  const handleTrigger = (updatedDagParams: DagParams) => {
    TriggerDag(updatedDagParams);
    onClose();
  };

  return (
    <Box>
      <Box alignSelf="center" cursor="pointer" onClick={onOpen}>
        <FiPlay />
      </Box>

      <Dialog.Root onOpenChange={onClose} open={open} size="xl">
        <Dialog.Content backdrop>
          <Dialog.Header>
            <VStack align="start" gap={2}>
              <Heading size="xl">Trigger DAG {dagDisplayName !== "" && (<span>- {dagDisplayName}</span>)}</Heading>
              {dagDisplayName === "" && (
                <Text color="gray.500" fontSize="md">
                  DAG ID: {dagId}
                </Text>
              )}
            </VStack>
          </Dialog.Header>

          <Dialog.CloseTrigger />

          <Dialog.Body>
            <TriggerDAGForm
              dagParams={dagParams}
              onClose={onClose}
              onTrigger={handleTrigger} 
              setDagParams={setDagParams}
            />
          </Dialog.Body>
        </Dialog.Content>
      </Dialog.Root>
    </Box>
  );
};

export default TriggerDAGModal;
