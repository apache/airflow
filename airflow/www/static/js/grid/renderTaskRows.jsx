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

import React, { useCallback } from 'react';
import {
  Tr,
  Td,
  Box,
  Flex,
  Collapse,
  useTheme,
} from '@chakra-ui/react';

import StatusBox, { boxSize, boxSizePx } from './components/StatusBox';
import TaskName from './components/TaskName';

import useSelection from './utils/useSelection';

const boxPadding = 3;
const boxPaddingPx = `${boxPadding}px`;
const columnWidth = boxSize + 2 * boxPadding;

const renderTaskRows = ({
  task, level = 0, ...rest
}) => task.children && task.children.map((t) => (
  <Row
    {...rest}
    key={t.id}
    task={t}
    level={level}
  />
));

const TaskInstances = ({
  task, dagRunIds, selectedRunId, onSelect, activeTaskState,
}) => (
  <Flex justifyContent="flex-end">
    {dagRunIds.map((runId) => {
      // Check if an instance exists for the run, or return an empty box
      const instance = task.instances.find((gi) => gi && gi.runId === runId);
      const isSelected = selectedRunId === runId;
      return (
        <Box
          py="4px"
          px={boxPaddingPx}
          className={`js-${runId}`}
          data-selected={isSelected}
          transition="background-color 0.2s"
          key={`${runId}-${task.id}`}
          bg={isSelected && 'blue.100'}
        >
          {instance
            ? (
              <StatusBox
                instance={instance}
                group={task}
                onSelect={onSelect}
                isActive={activeTaskState === undefined || activeTaskState === instance.state}
              />
            )
            : <Box width={boxSizePx} data-testid="blank-task" />}
        </Box>
      );
    })}
  </Flex>
);

const Row = (props) => {
  const {
    task,
    level,
    dagRunIds,
    openParentCount = 0,
    openGroupIds = [],
    onToggleGroups = () => {},
    hoveredTaskState,
  } = props;
  const { colors } = useTheme();
  const { selected, onSelect } = useSelection();

  const hoverBlue = `${colors.blue[100]}50`;
  const isGroup = !!task.children;
  const isSelected = selected.taskId === task.id;

  const isOpen = openGroupIds.some((g) => g === task.label);

  // assure the function is the same across renders
  const memoizedToggle = useCallback(
    () => {
      if (isGroup) {
        let newGroupIds = [];
        if (!isOpen) {
          newGroupIds = [...openGroupIds, task.label];
        } else {
          newGroupIds = openGroupIds.filter((g) => g !== task.label);
        }
        onToggleGroups(newGroupIds);
      }
    },
    [isGroup, isOpen, task.label, openGroupIds, onToggleGroups],
  );

  const isFullyOpen = level === openParentCount;

  return (
    <>
      <Tr
        bg={isSelected && 'blue.100'}
        borderBottomWidth={isFullyOpen ? 1 : 0}
        borderBottomColor={isGroup && isOpen ? 'gray.400' : 'gray.200'}
        role="group"
        _hover={!isSelected && { bg: hoverBlue }}
        transition="background-color 0.2s"
      >
        <Td
          bg={isSelected ? 'blue.100' : 'white'}
          _groupHover={!isSelected && ({ bg: 'blue.50' })}
          p={0}
          transition="background-color 0.2s"
          lineHeight="18px"
          position="sticky"
          left={0}
          borderBottom={0}
          width="100%"
          zIndex={1}
        >
          <Collapse in={isFullyOpen} unmountOnExit>
            <TaskName
              onToggle={memoizedToggle}
              isGroup={isGroup}
              isMapped={task.isMapped}
              label={task.label}
              isOpen={isOpen}
              level={level}
            />
          </Collapse>
        </Td>
        <Td width={0} p={0} borderBottom={0} />
        <Td
          p={0}
          align="right"
          width={`${dagRunIds.length * columnWidth}px`}
          borderBottom={0}
        >
          <Collapse in={isFullyOpen} unmountOnExit>
            <TaskInstances
              dagRunIds={dagRunIds}
              task={task}
              selectedRunId={selected.runId}
              onSelect={onSelect}
              activeTaskState={hoveredTaskState}
            />
          </Collapse>
        </Td>
      </Tr>
      {isGroup && (
        renderTaskRows({
          ...props, level: level + 1, openParentCount: openParentCount + (isOpen ? 1 : 0),
        })
      )}
    </>
  );
};

export default renderTaskRows;
