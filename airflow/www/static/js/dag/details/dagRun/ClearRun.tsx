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
import { useClearRun, useQueueRun } from "src/api";

const canEdit = getMetaValue("can_edit") === "True";
const dagId = getMetaValue("dag_id");

interface Props extends MenuButtonProps {
  runId: string;
}

const ClearRun = ({ runId, ...otherProps }: Props) => {
  const { mutateAsync: onClear, isLoading: isClearLoading } = useClearRun(
    dagId,
    runId
  );

  const { mutateAsync: onQueue, isLoading: isQueueLoading } = useQueueRun(
    dagId,
    runId
  );

  const clearExistingTasks = () => {
    onClear({ confirmed: true });
  };

  const queueNewTasks = () => {
    onQueue({ confirmed: true });
  };

  const clearLabel = "Clear tasks or add new tasks";
  return (
    <Menu>
      <MenuButton
        as={Button}
        colorScheme="blue"
        transition="all 0.2s"
        title={clearLabel}
        aria-label={clearLabel}
        disabled={!canEdit || isClearLoading || isQueueLoading}
        {...otherProps}
      >
        <Flex>
          Clear
          <MdArrowDropDown size="16px" />
        </Flex>
      </MenuButton>
      <MenuList>
        <MenuItem onClick={clearExistingTasks}>Clear existing tasks</MenuItem>
        <MenuItem onClick={queueNewTasks}>Queue up new tasks</MenuItem>
      </MenuList>
    </Menu>
  );
};

export default ClearRun;
