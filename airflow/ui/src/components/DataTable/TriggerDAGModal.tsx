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
  Modal,
  ModalOverlay,
  ModalContent,
  useDisclosure,
  Box,
  Text,
  Heading,
  VStack,
} from "@chakra-ui/react";
import React, { useState } from "react";
import { FiPlay } from "react-icons/fi";

import { TriggerDAG } from "./TriggerDAG";
import TriggerDAGForm from "./TriggerDAGForm";

type DagParams = {
  configJson: string;
  dagId: string;
  logicalDate: string;
  runId?: string;
};

type TriggerDAGModalProps = {
  dagDisplayName: string;
  dagId: string;
};

const TriggerDAGModal: React.FC<TriggerDAGModalProps> = ({
  dagDisplayName,
  dagId,
}) => {
  const { isOpen, onClose, onOpen } = useDisclosure();
  const [dagParams, setDagParams] = useState<DagParams>({
    configJson: "{}",
    dagId,
    logicalDate: "",
    runId: "",
  });

  const handleTrigger = () => {
    TriggerDAG(dagParams);
    onClose();
  };

  return (
    <Box>
      <Box alignSelf="center" cursor="pointer" onClick={onOpen}>
        <FiPlay />
      </Box>

      <Modal isOpen={isOpen} onClose={onClose} size="xl">
        <ModalOverlay />
        <ModalContent>
          <VStack align="start" p={5} spacing={2}>
            <Heading size="md">Trigger DAG</Heading>

            <Box>
              <Heading mb={1} size="sm">
                {dagDisplayName}
              </Heading>
              <Text color="gray.500" fontSize="xs">
                DAG ID: {dagId}
              </Text>
            </Box>
          </VStack>

          <TriggerDAGForm
            dagParams={dagParams}
            onClose={onClose}
            onTrigger={handleTrigger}
            setDagParams={setDagParams}
          />
        </ModalContent>
      </Modal>
    </Box>
  );
};

export default TriggerDAGModal;
