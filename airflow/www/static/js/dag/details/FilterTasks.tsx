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
import { Button, Menu, MenuButton, MenuItem, MenuList } from "@chakra-ui/react";
import useFilters from "src/dag/useFilters";

interface Props {
  taskId: string;
}

const FilterTasks = ({ taskId }: Props) => {
  const { onFilterTasksChange } = useFilters();

  const onFilterUpstream = () =>
    onFilterTasksChange({
      root: taskId,
      filterUpstream: true,
      filterDownstream: false,
    });

  const onFilterDownstream = () =>
    onFilterTasksChange({
      root: taskId,
      filterUpstream: false,
      filterDownstream: true,
    });

  const onFilterAll = () =>
    onFilterTasksChange({
      root: taskId,
      filterUpstream: true,
      filterDownstream: true,
    });

  return (
    <Menu>
      <MenuButton
        as={Button}
        variant="outline"
        colorScheme="blue"
        transition="all 0.2s"
      >
        Filter Tasks
      </MenuButton>
      <MenuList>
        <MenuItem onClick={onFilterUpstream}>Filter Upstream</MenuItem>
        <MenuItem onClick={onFilterDownstream}>Filter Downstream</MenuItem>
        <MenuItem onClick={onFilterAll}>Filter Upstream & Downstream</MenuItem>
      </MenuList>
    </Menu>
  );
};

export default FilterTasks;
