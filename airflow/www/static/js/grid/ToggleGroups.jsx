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

/* global localStorage, CustomEvent, document */

import React, { useState } from 'react';
import { Flex, IconButton } from '@chakra-ui/react';
import { MdExpand, MdCompress } from 'react-icons/md';

import { getMetaValue } from '../utils';

const dagId = getMetaValue('dag_id');

const getGroupIds = (groups) => {
  const groupIds = [];
  const checkTasks = (tasks) => tasks.forEach((task) => {
    if (task.children) {
      groupIds.push(task.label);
      checkTasks(task.children);
    }
  });
  checkTasks(groups);
  return groupIds;
};

const ToggleGroups = ({ groups }) => {
  const openGroupsKey = `${dagId}/open-groups`;
  const allGroupIds = getGroupIds(groups.children);
  const storedGroups = JSON.parse(localStorage.getItem(openGroupsKey)) || [];
  const [openGroupIds, setOpenGroupIds] = useState(storedGroups);

  const isExpandDisabled = allGroupIds.length === openGroupIds.length;
  const isCollapseDisabled = !openGroupIds.length;

  // Don't show button if the DAG has no task groups
  const hasGroups = groups.children.find((c) => !!c.children);
  if (!hasGroups) return null;

  const onExpand = () => {
    const closeEvent = new CustomEvent('toggleGroups', { detail: { dagId, openGroups: true } });
    document.dispatchEvent(closeEvent);
    localStorage.setItem(openGroupsKey, JSON.stringify(allGroupIds));
    setOpenGroupIds(allGroupIds);
  };

  const onCollapse = () => {
    const closeEvent = new CustomEvent('toggleGroups', { detail: { dagId, closeGroups: true } });
    document.dispatchEvent(closeEvent);
    localStorage.removeItem(openGroupsKey);
    setOpenGroupIds([]);
  };

  return (
    <Flex>
      <IconButton
        fontSize="2xl"
        onClick={onExpand}
        title="Expand all task groups"
        aria-label="Expand all task groups"
        icon={<MdExpand />}
        isDisabled={isExpandDisabled}
        mr={2}
      />
      <IconButton
        fontSize="2xl"
        onClick={onCollapse}
        title="Collapse all task groups"
        aria-label="Collapse all task groups"
        isDisabled={isCollapseDisabled}
        icon={<MdCompress />}
      />
    </Flex>
  );
};

export default ToggleGroups;
