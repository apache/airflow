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

import React, { ReactNode } from "react";
import {
  Button,
  Modal,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalHeader,
  ModalOverlay,
  ModalProps,
  Box,
  Text,
  Accordion,
  AccordionButton,
  AccordionPanel,
  AccordionItem,
  AccordionIcon,
} from "@chakra-ui/react";

import { useContainerRef } from "src/context/containerRef";
import { Table } from "src/components/Table";
import type { MinimalTaskInstance } from "src/types";

interface Props extends ModalProps {
  affectedTasks?: MinimalTaskInstance[];
  header: ReactNode | string;
  subheader?: ReactNode | string;
  submitButton: ReactNode;
}

const columns = [
  {
    Header: "Task name",
    accessor: "taskId",
  },
  {
    Header: "Map Index",
    accessor: "mapIndex",
  },
  {
    Header: "Run Id",
    accessor: "runId",
  },
];

const AffectedTasksTable = ({
  affectedTasks,
}: {
  affectedTasks: MinimalTaskInstance[];
}) => <Table data={affectedTasks} columns={columns} />;

const ActionModal = ({
  isOpen,
  onClose,
  children,
  header,
  subheader,
  affectedTasks = [],
  submitButton,
  ...otherProps
}: Props) => {
  const containerRef = useContainerRef();
  return (
    <Modal
      size="6xl"
      isOpen={isOpen}
      onClose={onClose}
      portalProps={{ containerRef }}
      blockScrollOnMount={false}
      {...otherProps}
    >
      <ModalOverlay />
      <ModalContent>
        <ModalHeader>{header}</ModalHeader>
        <ModalCloseButton />
        <ModalBody>
          <Box mb={3}>{subheader}</Box>
          <Box>
            {children}
            <Accordion allowToggle my={3}>
              <AccordionItem>
                <AccordionButton>
                  <Box flex="1" textAlign="left">
                    <Text as="strong" size="lg">
                      Affected Tasks: {affectedTasks?.length || 0}
                    </Text>
                  </Box>
                  <AccordionIcon />
                </AccordionButton>
                <AccordionPanel>
                  <Box maxHeight="35vh" overflowY="auto">
                    <AffectedTasksTable affectedTasks={affectedTasks} />
                  </Box>
                </AccordionPanel>
              </AccordionItem>
            </Accordion>
          </Box>
        </ModalBody>
        <ModalFooter justifyContent="space-between">
          <Button colorScheme="gray" onClick={onClose}>
            Cancel
          </Button>
          {submitButton}
        </ModalFooter>
      </ModalContent>
    </Modal>
  );
};

export default ActionModal;
