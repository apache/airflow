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

import React, { useState, useRef } from "react";
import {
  Alert,
  AlertIcon,
  Flex,
  Button,
  Menu,
  MenuButton,
  MenuItem,
  MenuList,
  MenuButtonProps,
  useDisclosure,
  ButtonGroup,
  Box,
  Text,
} from "@chakra-ui/react";
import { MdArrowDropDown } from "react-icons/md";
import { capitalize } from "lodash";

import { getMetaValue } from "src/utils";
import { useKeysPress } from "src/utils/useKeysPress";
import type { TaskState } from "src/types";
import keyboardShortcutIdentifier from "src/dag/keyboardShortcutIdentifier";
import {
  useMarkFailedTask,
  useMarkSuccessTask,
  useMarkTaskDryRun,
} from "src/api";

import { SimpleStatus } from "../../../StatusBox";
import ActionButton from "./ActionButton";
import ActionModal from "./ActionModal";

const canEdit = getMetaValue("can_edit") === "True";
const dagId = getMetaValue("dag_id");

interface Props {
  runId: string;
  taskId: string;
  state?: TaskState;
  isGroup?: boolean;
  mapIndex?: number;
  isMapped?: boolean;
}

interface ModalProps extends Props {
  isOpen: boolean;
  onClose: () => void;
  state: TaskState;
}

const MarkAsModal = ({
  runId,
  taskId,
  isGroup,
  mapIndex,
  isMapped,
  state,
  isOpen,
  onClose,
}: ModalProps) => {
  const [past, setPast] = useState(false);
  const onTogglePast = () => setPast(!past);

  const [future, setFuture] = useState(false);
  const onToggleFuture = () => setFuture(!future);

  const [upstream, setUpstream] = useState(false);
  const onToggleUpstream = () => setUpstream(!upstream);

  const [downstream, setDownstream] = useState(false);
  const onToggleDownstream = () => setDownstream(!downstream);

  const initialMarkAsButtonFocusRef = useRef<HTMLButtonElement>(null);

  const mapIndexes =
    mapIndex !== undefined && mapIndex !== -1 ? [mapIndex] : undefined;

  const { data: affectedTasks, isLoading: isLoadingDryRun } = useMarkTaskDryRun(
    {
      dagId,
      runId,
      taskId,
      state,
      isGroup: !!isGroup,
      past,
      future,
      upstream,
      downstream,
      mapIndexes,
      enabled: isOpen,
    }
  );

  const { mutateAsync: markFailedMutation, isLoading: isMarkFailedLoading } =
    useMarkFailedTask({
      dagId,
      runId,
      taskId,
      isGroup: !!isGroup,
    });

  const { mutateAsync: markSuccessMutation, isLoading: isMarkSuccessLoading } =
    useMarkSuccessTask({
      dagId,
      runId,
      taskId,
      isGroup: !!isGroup,
    });

  const resetModal = () => {
    onClose();
    setDownstream(false);
    setUpstream(false);
    setPast(false);
    setFuture(false);
  };

  const onMarkState = () => {
    if (state === "success") {
      markSuccessMutation({
        past,
        future,
        upstream,
        downstream,
        mapIndexes,
      });
    } else if (state === "failed") {
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

  return (
    <ActionModal
      isOpen={isOpen}
      onClose={resetModal}
      header={`Mark as ${capitalize(state || "")}`}
      subheader={
        <>
          <Text>
            <Text as="strong" mr={1}>
              Task:
            </Text>
            {taskId}
          </Text>
          <Text>
            <Text as="strong" mr={1}>
              Run:
            </Text>
            {runId}
          </Text>
          {isMapped && (
            <Text>
              <Text as="strong" mr={1}>
                Map Index:
              </Text>
              {mapIndex !== undefined ? mapIndex : `All mapped tasks`}
            </Text>
          )}
        </>
      }
      affectedTasks={affectedTasks}
      submitButton={
        <Button
          ref={initialMarkAsButtonFocusRef}
          colorScheme={
            (state === "success" && "green") ||
            (state === "failed" && "red") ||
            "grey"
          }
          isLoading={
            isLoadingDryRun || isMarkSuccessLoading || isMarkFailedLoading
          }
          isDisabled={!affectedTasks?.length || !state}
          onClick={onMarkState}
        >
          Mark as {state}
        </Button>
      }
      initialFocusRef={initialMarkAsButtonFocusRef}
    >
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
      </Box>
      {isGroup && (past || future) && (
        <Alert status="warning" my={3}>
          <AlertIcon />
          Marking a TaskGroup as {capitalize(state || "")} in the future and/or
          past will affect all the tasks of this group across multiple dag runs.
          <br />
          This can take a while to complete.
        </Alert>
      )}
    </ActionModal>
  );
};

const MarkInstanceAs = ({
  runId,
  taskId,
  isGroup,
  mapIndex,
  isMapped,
  state: currentState,
  ...otherProps
}: Props & MenuButtonProps) => {
  const { onOpen, onClose, isOpen } = useDisclosure();

  const [newState, setNewState] = useState<"failed" | "success">("success");

  const markAsFailed = () => {
    setNewState("failed");
    onOpen();
  };

  const markAsSuccess = () => {
    setNewState("success");
    onOpen();
  };

  const markLabel = "Manually set task instance state";
  const isMappedSummary = isMapped && mapIndex === undefined;

  useKeysPress(keyboardShortcutIdentifier.taskMarkSuccess, () => {
    if (1 - Number(!isMappedSummary && currentState === "success"))
      markAsSuccess();
  });

  useKeysPress(keyboardShortcutIdentifier.taskMarkFailed, () => {
    if (1 - Number(!isMappedSummary && currentState === "failed"))
      markAsFailed();
  });

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
          <MenuItem onClick={markAsFailed}>
            <SimpleStatus state="failed" mr={2} />
            failed
          </MenuItem>
          <MenuItem onClick={markAsSuccess}>
            <SimpleStatus state="success" mr={2} />
            success
          </MenuItem>
        </MenuList>
      </Menu>
      {/* Only load modal is user can edit */}
      {canEdit && (
        <MarkAsModal
          runId={runId}
          taskId={taskId}
          isGroup={isGroup}
          mapIndex={mapIndex}
          isMapped={isMapped}
          state={newState}
          isOpen={isOpen}
          onClose={onClose}
        />
      )}
    </>
  );
};

export default MarkInstanceAs;
