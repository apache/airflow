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
  Box,
  Button,
  ButtonGroup,
  ButtonProps,
  Text,
  useDisclosure,
} from "@chakra-ui/react";

import { getMetaValue } from "src/utils";
import { useKeysPress } from "src/utils/useKeysPress";
import { useClearTask } from "src/api";
import useClearTaskDryRun from "src/api/useClearTaskDryRun";
import keyboardShortcutIdentifier from "src/dag/keyboardShortcutIdentifier";

import ActionButton from "./ActionButton";
import ActionModal from "./ActionModal";

const canEditTaskInstance = getMetaValue("can_edit_taskinstance") === "True";
const canEdit = getMetaValue("can_edit") === "True";
const dagId = getMetaValue("dag_id");

interface Props {
  runId: string;
  taskId: string;
  executionDate: string;
  isGroup?: boolean;
  isMapped?: boolean;
  mapIndex?: number;
}

interface ClearModalProps extends Props {
  isOpen: boolean;
  onClose: () => void;
}

const ClearModal = ({
  runId,
  taskId,
  mapIndex,
  executionDate,
  isGroup,
  isMapped,
  isOpen,
  onClose,
}: ClearModalProps) => {
  const [past, setPast] = useState(false);
  const onTogglePast = () => setPast(!past);

  const [future, setFuture] = useState(false);
  const onToggleFuture = () => setFuture(!future);

  const [upstream, setUpstream] = useState(false);
  const onToggleUpstream = () => setUpstream(!upstream);

  const [downstream, setDownstream] = useState(true);
  const onToggleDownstream = () => setDownstream(!downstream);

  const [recursive, setRecursive] = useState(true);
  const onToggleRecursive = () => setRecursive(!recursive);

  const [failed, setFailed] = useState(false);
  const onToggleFailed = () => setFailed(!failed);

  const initialClearButtonFocusRef = useRef<HTMLButtonElement>(null);

  const mapIndexes =
    mapIndex !== undefined && mapIndex !== -1 ? [mapIndex] : undefined;

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
      mapIndexes,
      enabled: isOpen,
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
    setDownstream(true);
    setUpstream(false);
    setPast(false);
    setFuture(false);
    setRecursive(true);
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
      mapIndexes,
    });
    resetModal();
  };

  return (
    <ActionModal
      isOpen={isOpen}
      onClose={resetModal}
      header="Clear and Retry"
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
          ref={initialClearButtonFocusRef}
          colorScheme="blue"
          isLoading={isLoading || isLoadingDryRun}
          isDisabled={!affectedTasks?.length}
          onClick={onClear}
        >
          Clear
        </Button>
      }
      initialFocusRef={initialClearButtonFocusRef}
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
          <ActionButton
            bg={recursive ? "gray.100" : undefined}
            onClick={onToggleRecursive}
            name="Recursive"
          />
          <ActionButton
            bg={failed ? "gray.100" : undefined}
            onClick={onToggleFailed}
            name="Failed"
          />
        </ButtonGroup>
      </Box>
      {isGroup && (past || future) && (
        <Alert status="warning" my={3}>
          <AlertIcon />
          Clearing a TaskGroup in the future and/or past will affect all the
          tasks of this group across multiple dag runs.
          <br />
          This can take a while to complete.
        </Alert>
      )}
    </ActionModal>
  );
};

const ClearInstance = ({
  runId,
  taskId,
  mapIndex,
  executionDate,
  isGroup,
  isMapped,
  ...otherProps
}: Props & ButtonProps) => {
  const { onOpen, onClose, isOpen } = useDisclosure();

  useKeysPress(keyboardShortcutIdentifier.taskRunClear, onOpen);

  const clearLabel = "Clear and retry task.";

  return (
    <>
      <Button
        title={clearLabel}
        aria-label={clearLabel}
        isDisabled={!canEdit || !canEditTaskInstance}
        colorScheme="blue"
        onClick={onOpen}
        {...otherProps}
      >
        Clear task
      </Button>
      {/* Only mount modal if user can edit */}
      {canEdit && canEditTaskInstance && (
        <ClearModal
          runId={runId}
          taskId={taskId}
          mapIndex={mapIndex}
          executionDate={executionDate}
          isGroup={isGroup}
          isMapped={isMapped}
          isOpen={isOpen}
          onClose={onClose}
        />
      )}
    </>
  );
};

export default ClearInstance;
