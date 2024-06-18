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
} from "@chakra-ui/react";
import useFilters from "src/dag/useFilters";
import { MdArrowDropDown } from "react-icons/md";
import { useGridData } from "src/api";
import { getTask } from "src/utils";

interface Props {
  taskId: string | null;
}

const FilterTasks = ({ taskId }: Props) => {
  const {
    filters: { root },
    onFilterTasksChange,
    resetRoot,
  } = useFilters();

  const {
    data: { groups },
  } = useGridData();

  const label = "Filter upstream and/or downstream of a task";

  if (!root && !taskId) return null;

  const onFilterUpstream = () => {
    if (taskId)
      onFilterTasksChange({
        root: taskId,
        filterUpstream: true,
        filterDownstream: false,
      });
  };

  const onFilterDownstream = () => {
    if (taskId)
      onFilterTasksChange({
        root: taskId,
        filterUpstream: false,
        filterDownstream: true,
      });
  };

  const onFilterAll = () => {
    if (taskId)
      onFilterTasksChange({
        root: taskId,
        filterUpstream: true,
        filterDownstream: true,
      });
  };

  const task = root ? getTask({ taskId: root, task: groups }) : undefined;

  return (
    <Menu>
      <MenuButton
        as={Button}
        variant={root ? "solid" : "outline"}
        colorScheme="blue"
        transition="all 0.2s"
        title={label}
        aria-label={label}
        mt={2}
        ml={2}
      >
        <Flex>
          {root
            ? `Filtered on ${task?.label || "unknown"}`
            : "Filter DAG by task"}
          <MdArrowDropDown size="16px" />
        </Flex>
      </MenuButton>
      <MenuList>
        {!!root && <MenuItem onClick={resetRoot}>Reset</MenuItem>}
        {!!taskId && (
          <>
            <MenuItem onClick={onFilterUpstream}>Only upstream</MenuItem>
            <MenuItem onClick={onFilterDownstream}>Only downstream</MenuItem>
            <MenuItem onClick={onFilterAll}>
              Both upstream & downstream
            </MenuItem>
          </>
        )}
      </MenuList>
    </Menu>
  );
};

export default FilterTasks;
