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
  Flex,
  Button,
  Menu,
  MenuButton,
  MenuItem,
  MenuList,
  MenuButtonProps,
  Modal,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalHeader,
  ModalOverlay,
  useDisclosure,
  ButtonGroup,
  Box,
  Text,
  Accordion,
  AccordionButton,
  AccordionPanel,
  AccordionItem,
  AccordionIcon,
  Code,
} from "@chakra-ui/react";
import { MdArrowDropDown } from "react-icons/md";

import { getMetaValue } from "src/utils";
import type { TaskState } from "src/types";
import { useContainerRef } from "src/context/containerRef";
import {
  useMarkFailedTask,
  useMarkSuccessTask,
  useMarkTaskDryRun,
} from "src/api";

import { SimpleStatus } from "../../StatusBox";
import ActionButton from "./taskActions/ActionButton";

const canEdit = getMetaValue("can_edit") === "True";
const dagId = getMetaValue("dag_id");

interface Props extends MenuButtonProps {
  runId: string;
  taskId: string;
  state?: TaskState;
  mapIndexes?: number[];
}

const MarkInstanceAs = ({
  runId,
  taskId,
  mapIndexes,
  state: currentState,
  ...otherProps
}: Props) => {
  const { onOpen, onClose, isOpen } = useDisclosure();
  const containerRef = useContainerRef();

  const [newState, setNewState] = useState<"failed" | "success">("success");

  const [past, setPast] = useState(false);
  const onTogglePast = () => setPast(!past);

  const [future, setFuture] = useState(false);
  const onToggleFuture = () => setFuture(!future);

  const [upstream, setUpstream] = useState(false);
  const onToggleUpstream = () => setUpstream(!upstream);

  const [downstream, setDownstream] = useState(false);
  const onToggleDownstream = () => setDownstream(!downstream);

  const markAsFailed = () => {
    setNewState("failed");
    onOpen();
  };

  const markAsSuccess = () => {
    setNewState("success");
    onOpen();
  };

  const { data: affectedTasks, isLoading: isLoadingDryRun } = useMarkTaskDryRun(
    {
      dagId,
      runId,
      taskId,
      state: newState,
      past,
      future,
      upstream,
      downstream,
      mapIndexes,
    }
  );

  const { mutateAsync: markFailedMutation, isLoading: isMarkFailedLoading } =
    useMarkFailedTask({
      dagId,
      runId,
      taskId,
    });

  const { mutateAsync: markSuccessMutation, isLoading: isMarkSuccessLoading } =
    useMarkSuccessTask({
      dagId,
      runId,
      taskId,
    });

  const resetModal = () => {
    onClose();
    setDownstream(false);
    setUpstream(false);
    setPast(false);
    setFuture(false);
  };

  const onMarkState = () => {
    if (newState === "success") {
      markSuccessMutation({
        past,
        future,
        upstream,
        downstream,
        mapIndexes,
      });
    } else if (newState === "failed") {
      markFailedMutation({
        past,
        future,
        upstream,
        downstream,
        mapIndexes,
      });
    }
    resetModal();
  };

  const markLabel = "Manually set task instance state";
  return (
    <>
      <Menu>
        <MenuButton
          as={Button}
          colorScheme="blue"
          transition="all 0.2s"
          title={markLabel}
          aria-label={markLabel}
          disabled={!canEdit}
          {...otherProps}
        >
          <Flex>
            Mark state as…
            <MdArrowDropDown size="16px" />
          </Flex>
        </MenuButton>
        <MenuList>
          <MenuItem
            onClick={markAsFailed}
            isDisabled={currentState === "failed"}
          >
            <SimpleStatus state="failed" mr={2} />
            failed
          </MenuItem>
          <MenuItem
            onClick={markAsSuccess}
            isDisabled={currentState === "success"}
          >
            <SimpleStatus state="success" mr={2} />
            success
          </MenuItem>
        </MenuList>
      </Menu>
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
            Mark {taskId} as {newState}
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
              colorScheme={
                (newState === "success" && "green") ||
                (newState === "failed" && "red") ||
                "grey"
              }
              isLoading={
                isLoadingDryRun || isMarkSuccessLoading || isMarkFailedLoading
              }
              isDisabled={!affectedTasks?.length || !newState}
              onClick={onMarkState}
            >
              Mark as {newState}
            </Button>
          </ModalFooter>
        </ModalContent>
      </Modal>
    </>
  );
};

export default MarkInstanceAs;
