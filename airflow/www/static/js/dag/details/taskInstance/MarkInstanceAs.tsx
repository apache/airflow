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
import type { TaskState } from "src/types";

import { SimpleStatus } from "../../StatusBox";

const canEdit = getMetaValue("can_edit") === "True";

interface Props extends MenuButtonProps {
  runId: string;
  taskId: string;
  state?: TaskState;
}

const MarkInstanceAs = ({ runId, taskId, state, ...otherProps }: Props) => {
  const markAsFailed = () => {
    console.log(`Setting ${runId}.${taskId} as failed`);
  };

  const markAsSuccess = () => {
    console.log(`Setting ${runId}.${taskId} as success`);
  };

  const markLabel = "Manually set task instance state";
  return (
    <Menu>
      <MenuButton
        as={Button}
        variant="outline"
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

export default MarkInstanceAs;
