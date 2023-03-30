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

import React, { useState } from "react";
import {
  Box,
  Button,
  ButtonGroup,
  ButtonProps,
  Code,
  Modal,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalHeader,
  ModalOverlay,
  Text,
  useDisclosure,
  Accordion,
  AccordionButton,
  AccordionPanel,
  AccordionItem,
  AccordionIcon,
} from "@chakra-ui/react";

import { getMetaValue } from "src/utils";
import { useContainerRef } from "src/context/containerRef";
import { useClearTask } from "src/api";
import useClearTaskDryRun from "src/api/useClearTaskDryRun";
import Time from "src/components/Time";

import ActionButton from "./taskActions/ActionButton";

const canEdit = getMetaValue("can_edit") === "True";
const dagId = getMetaValue("dag_id");

interface Props extends ButtonProps {
  runId: string;
  taskId: string;
  executionDate: string;
  isGroup?: boolean;
  isSubDag?: boolean;
  mapIndex?: number;
}

const ClearInstance = ({
  runId,
  taskId,
  mapIndex,
  executionDate,
  isGroup,
  isSubDag,
  ...otherProps
}: Props) => {
  const { onOpen, onClose, isOpen } = useDisclosure();
  const containerRef = useContainerRef();

  const [past, setPast] = useState(false);
  const onTogglePast = () => setPast(!past);

  const [future, setFuture] = useState(false);
  const onToggleFuture = () => setFuture(!future);

  const [upstream, setUpstream] = useState(false);
  const onToggleUpstream = () => setUpstream(!upstream);

  const [downstream, setDownstream] = useState(false);
  const onToggleDownstream = () => setDownstream(!downstream);

  const [recursive, setRecursive] = useState(false);
  const onToggleRecursive = () => setRecursive(!recursive);

  const [failed, setFailed] = useState(false);
  const onToggleFailed = () => setFailed(!failed);

  const { data: affectedTasks, isLoading: isLoadingDryRun } =
    useClearTaskDryRun({
      dagId,
      runId,
      taskId,
      executionDate,
      isGroup: !!isGroup,
      past,
      future,
      upstream,
      downstream,
      recursive,
      failed,
      mapIndexes: [mapIndex || -1],
    });

  const { mutateAsync: clearTask, isLoading } = useClearTask({
    dagId,
    runId,
    taskId,
    executionDate,
    isGroup: !!isGroup,
  });

  const resetModal = () => {
    onClose();
    setDownstream(false);
    setUpstream(false);
    setPast(false);
    setFuture(false);
    setRecursive(false);
    setFailed(false);
  };

  const onClear = () => {
    clearTask({
      confirmed: true,
      past,
      future,
      upstream,
      downstream,
      recursive,
      failed,
      mapIndexes: [mapIndex || -1],
    });
    resetModal();
  };

  const clearLabel = "Clear and retry task.";
  return (
    <>
      <Button
        title={clearLabel}
        aria-label={clearLabel}
        ml={2}
        isDisabled={!canEdit}
        colorScheme="blue"
        onClick={onOpen}
        {...otherProps}
      >
        Clear task
      </Button>
      <Modal
        size="3xl"
        isOpen={isOpen}
        onClose={resetModal}
        portalProps={{ containerRef }}
        blockScrollOnMount={false}
      >
        <ModalOverlay />
        <ModalContent>
          <ModalHeader>
            Clear and retry {taskId} at <Time dateTime={executionDate} />
            {mapIndex && mapIndex > -1 ? `[${mapIndex}]` : ""}
          </ModalHeader>
          <ModalCloseButton />
          <ModalBody>
            <Box>
              <Text>Include: </Text>
              <ButtonGroup isAttached variant="outline" isDisabled={!canEdit}>
                <ActionButton
                  bg={past ? "gray.100" : undefined}
                  onClick={onTogglePast}
                  name="Past"
                />
                <ActionButton
                  bg={future ? "gray.100" : undefined}
                  onClick={onToggleFuture}
                  name="Future"
                />
                <ActionButton
                  bg={upstream ? "gray.100" : undefined}
                  onClick={onToggleUpstream}
                  name="Upstream"
                />
                <ActionButton
                  bg={downstream ? "gray.100" : undefined}
                  onClick={onToggleDownstream}
                  name="Downstream"
                />
                {isSubDag && (
                  <ActionButton
                    bg={recursive ? "gray.100" : undefined}
                    onClick={onToggleRecursive}
                    name="Recursive"
                  />
                )}
                <ActionButton
                  bg={failed ? "gray.100" : undefined}
                  onClick={onToggleFailed}
                  name="Failed"
                />
              </ButtonGroup>
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
                    <Box maxHeight="400px" overflowY="auto">
                      {(affectedTasks || []).map((ti) => (
                        <Code width="100%" key={ti} fontSize="lg">
                          {ti}
                        </Code>
                      ))}
                    </Box>
                  </AccordionPanel>
                </AccordionItem>
              </Accordion>
            </Box>
          </ModalBody>
          <ModalFooter justifyContent="space-between">
            <Button colorScheme="gray" onClick={resetModal}>
              Cancel
            </Button>
            <Button
              colorScheme="blue"
              isLoading={isLoading || isLoadingDryRun}
              isDisabled={!affectedTasks?.length}
              onClick={onClear}
            >
              Clear
            </Button>
          </ModalFooter>
        </ModalContent>
      </Modal>
    </>
  );
};

export default ClearInstance;
