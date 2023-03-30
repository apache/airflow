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
  useDisclosure,
  ButtonGroup,
  Box,
  Text,
} from "@chakra-ui/react";
import { MdArrowDropDown } from "react-icons/md";
import { capitalize } from "lodash";

import { getMetaValue } from "src/utils";
import type { TaskState } from "src/types";
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

interface Props extends MenuButtonProps {
  runId: string;
  taskId: string;
  state?: TaskState;
  mapIndex?: number;
  isMapped?: boolean;
}

const MarkInstanceAs = ({
  runId,
  taskId,
  mapIndex,
  isMapped,
  state: currentState,
  ...otherProps
}: Props) => {
  const { onOpen, onClose, isOpen } = useDisclosure();

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

  const mapIndexes =
    mapIndex !== undefined && mapIndex !== -1 ? [mapIndex] : undefined;

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
  const isMappedSummary = isMapped && mapIndex === undefined;

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
            isDisabled={!isMappedSummary && currentState === "failed"}
          >
            <SimpleStatus state="failed" mr={2} />
            failed
          </MenuItem>
          <MenuItem
            onClick={markAsSuccess}
            isDisabled={!isMappedSummary && currentState === "success"}
          >
            <SimpleStatus state="success" mr={2} />
            success
          </MenuItem>
        </MenuList>
      </Menu>
      <ActionModal
        isOpen={isOpen}
        onClose={resetModal}
        header={`Mark as ${capitalize(newState)}`}
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
        }
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
      </ActionModal>
    </>
  );
};

export default MarkInstanceAs;
