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

/* global localStorage */

import React, { useCallback } from 'react';
import {
  Tr,
  Td,
  Box,
  Flex,
  useDisclosure,
  Collapse,
} from '@chakra-ui/react';

import StatusBox from './StatusBox';
import TaskName from './TaskName';

import { getMetaValue } from '../utils';

// dagId comes from dag.html
const dagId = getMetaValue('dag_id');

const renderTaskRows = ({
  task, containerRef, level = 0, isParentOpen, dagRunIds,
}) => task.children.map((t) => (
  <Row
    key={t.id}
    task={t}
    level={level}
    containerRef={containerRef}
    prevTaskId={task.id}
    isParentOpen={isParentOpen}
    dagRunIds={dagRunIds}
  />
));

const TaskInstances = ({ task, containerRef, dagRunIds }) => (
  <Flex justifyContent="flex-end">
    {dagRunIds.map((runId) => {
      // Check if an instance exists for the run, or return an empty box
      const instance = task.instances.find((gi) => gi.runId === runId);
      return instance
        ? (
          <StatusBox
            key={`${runId}-${task.id}`}
            instance={instance}
            containerRef={containerRef}
            extraLinks={task.extraLinks}
            group={task}
          />
        )
        : <Box key={`${runId}-${task.id}`} width="18px" data-testid="blank-task" />;
    })}
  </Flex>
);

const Row = (props) => {
  const {
    task, containerRef, level, prevTaskId, isParentOpen = true, dagRunIds,
  } = props;
  const isGroup = !!task.children;

  const taskName = prevTaskId ? task.id.replace(`${prevTaskId}.`, '') : task.id;

  const storageKey = `${dagId}-open-groups`;
  const openGroups = JSON.parse(localStorage.getItem(storageKey)) || [];
  const isGroupId = openGroups.some((g) => g === taskName);
  const onOpen = () => {
    localStorage.setItem(storageKey, JSON.stringify([...openGroups, taskName]));
  };
  const onClose = () => {
    localStorage.setItem(storageKey, JSON.stringify(openGroups.filter((g) => g !== taskName)));
  };
  const { isOpen, onToggle } = useDisclosure({ defaultIsOpen: isGroupId, onClose, onOpen });

  // assure the function is the same across renders
  const memoizedToggle = useCallback(
    () => isGroup && onToggle(),
    [onToggle, isGroup],
  );

  const parentTasks = task.id.split('.');
  parentTasks.splice(-1);

  const isFullyOpen = isParentOpen && parentTasks.every((p) => openGroups.some((g) => g === p));

  return (
    <>
      <Tr
        backgroundColor={`rgba(203, 213, 224, ${0.25 * level})`}
        borderBottomWidth={isFullyOpen ? 1 : 0}
        borderBottomColor={level > 1 ? 'white' : 'gray.200'}
        role="group"
      >
        <Td
          _groupHover={level > 3 && {
            color: 'white',
          }}
          p={0}
          lineHeight="18px"
          position="sticky"
          left={0}
          backgroundColor="white"
          borderBottom={0}
        >
          <Collapse in={isFullyOpen} unmountOnExit>
            <TaskName
              onToggle={memoizedToggle}
              isGroup={isGroup}
              isMapped={task.isMapped}
              taskName={taskName}
              isOpen={isOpen}
              level={level}
            />
          </Collapse>
        </Td>
        <Td width={0} p={0} borderBottom={0} />
        <Td p={0} align="right" _groupHover={{ backgroundColor: 'rgba(113, 128, 150, 0.1)' }} transition="background-color 0.2s" borderBottom={0}>
          <Collapse in={isFullyOpen} unmountOnExit>
            <TaskInstances dagRunIds={dagRunIds} task={task} containerRef={containerRef} />
          </Collapse>
        </Td>
      </Tr>
      {isGroup && (
        renderTaskRows({
          ...props, level: level + 1, isParentOpen: isOpen,
        })
      )}
    </>
  );
};

export default renderTaskRows;
