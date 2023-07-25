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

import React from "react";
import {
  Flex,
  Button,
  Menu,
  MenuButton,
  MenuItem,
  MenuList,
  MenuButtonProps,
} from "@chakra-ui/react";
import { MdArrowDropDown } from "react-icons/md";
import { getMetaValue } from "src/utils";
import { useKeysPress } from "src/utils/useKeysPress";
import keyboardShortcutIdentifier from "src/dag/keyboardShortcutIdentifier";
import { useMarkFailedRun, useMarkSuccessRun } from "src/api";
import type { RunState } from "src/types";

import { SimpleStatus } from "../../StatusBox";

const canEdit = getMetaValue("can_edit") === "True";
const dagId = getMetaValue("dag_id");

interface Props extends MenuButtonProps {
  runId: string;
  state?: RunState;
}

const MarkRunAs = ({ runId, state, ...otherProps }: Props) => {
  const { mutateAsync: markFailed, isLoading: isMarkFailedLoading } =
    useMarkFailedRun(dagId, runId);
  const { mutateAsync: markSuccess, isLoading: isMarkSuccessLoading } =
    useMarkSuccessRun(dagId, runId);

  const markAsFailed = () => {
    markFailed({ confirmed: true });
  };

  const markAsSuccess = () => {
    markSuccess({ confirmed: true });
  };

  useKeysPress(keyboardShortcutIdentifier.dagMarkSuccess, () => {
    if (state !== "success") markAsSuccess();
  });
  useKeysPress(keyboardShortcutIdentifier.dagMarkFailed, () => {
    if (state !== "failed") markAsFailed();
  });

  const markLabel = "Manually set dag run state";
  return (
    <Menu>
      <MenuButton
        as={Button}
        colorScheme="blue"
        transition="all 0.2s"
        title={markLabel}
        aria-label={markLabel}
        disabled={!canEdit || isMarkFailedLoading || isMarkSuccessLoading}
        {...otherProps}
        mt={2}
      >
        <Flex>
          Mark state as...
          <MdArrowDropDown size="16px" />
        </Flex>
      </MenuButton>
      <MenuList>
        <MenuItem onClick={markAsFailed} isDisabled={state === "failed"}>
          <SimpleStatus state="failed" mr={2} />
          failed
        </MenuItem>
        <MenuItem onClick={markAsSuccess} isDisabled={state === "success"}>
          <SimpleStatus state="success" mr={2} />
          success
        </MenuItem>
      </MenuList>
    </Menu>
  );
};

export default MarkRunAs;
