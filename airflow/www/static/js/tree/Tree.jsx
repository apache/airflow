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

import React, { useRef, useState } from 'react';
import {
  Flex,
  Box,
  Text,
  useDisclosure,
  Collapse,
  Switch,
  FormControl,
  FormLabel,
  Spinner,
} from '@chakra-ui/react';
import { FiChevronUp, FiChevronDown } from 'react-icons/fi';

import useTreeData from './useTreeData';
import StatusBox from './StatusBox';
import DagRunBar from './DagRunBar';
import getMetaValue from '../meta_value';
import { getDuration, formatDuration } from './utils';

// dagId comes from dag.html
const dagId = getMetaValue('dag_id');

const DurationTick = ({ children, ...rest }) => (
  <Text fontSize={10} color="gray.400" position="absolute" left="calc(100% + 6px)" whiteSpace="nowrap" {...rest}>
    {children}
  </Text>
);

const Tree = () => {
  const containerRef = useRef();
  const { data, refreshOn, onToggleRefresh } = useTreeData();
  const { groups = [], dagRuns = [] } = data;
  const durations = [];
  const runs = dagRuns.map((dagRun) => {
    const duration = getDuration(dagRun.startDate, dagRun.endDate);
    durations.push(duration);
    return {
      ...dagRun,
      duration,
    };
  });

  // calculate dag run bar heights relative to max
  const max = Math.max.apply(null, durations);

  const renderGroup = (prevGroup, level) => prevGroup.children.map((group) => (
    <TaskGroup key={group.id} group={group} level={level} prevGroup={prevGroup.id} />
  ));

  const TaskGroup = ({ prevGroup, group, level }) => {
    const isGroup = !!group.children;

    const storageKey = `${dagId}-open-groups`;
    const openGroups = JSON.parse(localStorage.getItem(storageKey)) || [];
    const isGroupId = openGroups.some((g) => g === group.id);

    const onOpen = () => {
      localStorage.setItem(storageKey, JSON.stringify([...openGroups, group.id]));
    };
    const onClose = () => {
      localStorage.setItem(storageKey, JSON.stringify(openGroups.filter((g) => g !== group.id)));
    };

    const { isOpen, onToggle } = useDisclosure({ defaultIsOpen: isGroupId, onClose, onOpen });
    const [isHover, setHover] = useState(false);
    // Show only the immediate task id
    const taskName = group.id.replace(`${prevGroup}.`, '');

    return (
      <Box backgroundColor={`rgba(203, 213, 224, ${0.25 * level})`}>
        <Flex
          justifyContent="space-between"
          alignItems="center"
          width="100%"
          py="1px"
          borderBottomWidth={1}
          borderBottomColor={level > 1 ? 'white' : 'gray.200'}
          backgroundColor={isHover && 'rgba(113, 128, 150, 0.1)'}
          onMouseEnter={() => setHover(true)}
          onMouseLeave={() => setHover(false)}
        >
          <Flex
            as={isGroup ? 'button' : 'div'}
            onClick={() => isGroup && onToggle()}
            color={(level > 4 || (isHover && level > 3)) && 'white'}
            minWidth="250px"
            maxWidth="250px"
            aria-label={taskName}
            title={taskName}
          >
            <Text
              display="inline"
              fontSize={12}
              ml={`${level * 10 + 10}px`}
              isTruncated
            >
              {taskName}
            </Text>
            {isGroup && (
              isOpen ? <FiChevronDown /> : <FiChevronUp />
            )}
          </Flex>
          <Flex justifyContent="flex-end" overflowX="scroll" overflowY="visible">
            {runs.reverse().map((run) => {
              // Check if an instance exists for the run, or return an empty box
              const instance = group.instances.find((gi) => gi.runId === run.runId);
              return (
                instance
                  ? (
                    <StatusBox
                      key={`${instance.runId}-${instance.taskId}`}
                      instance={instance}
                      containerRef={containerRef}
                      group={group}
                    />
                  )
                  : <Box width="18px" key={`${run.runId}-${group.id}`} />
              );
            })}
          </Flex>
        </Flex>
        <Collapse in={isOpen} animateOpacity>
          {isGroup && renderGroup(group, level + 1)}
        </Collapse>
      </Box>
    );
  };

  return (
    <Box>
      <FormControl display="flex" alignItems="center" justifyContent="flex-end" width="100%" mb={10}>
        {refreshOn && <Spinner color="blue.500" speed="1s" mr={2} />}
        <FormLabel htmlFor="auto-refresh" mb={0} fontSize={12} fontWeight="normal">
          Auto-refresh
        </FormLabel>
        <Switch id="auto-refresh" onChange={onToggleRefresh} isChecked={refreshOn} size="lg" />
      </FormControl>
      <Box py={10} position="relative" mr={50} pl={10} borderRightWidth={1}>
        <Text transform="rotate(-90deg)" position="absolute" left="-6px" top="30px">Runs</Text>
        <Text transform="rotate(-90deg)" position="absolute" left="-6px" top="135px">Tasks</Text>
        {!!runs.length && (
        <>
          <DurationTick top={-1}>Duration</DurationTick>
          <DurationTick top={6}>
            {formatDuration(max)}
          </DurationTick>
          <Box position="absolute" top="22px" borderBottomWidth={1} zIndex={0} opacity={0.7} width="calc(100% - 20px)" />
          <DurationTick top={65}>
            {formatDuration(max / 2)}
          </DurationTick>
          <Box position="absolute" top="72px" borderBottomWidth={1} zIndex={0} opacity={0.7} width="calc(100% - 20px)" />
          <DurationTick top={118}>
            00:00:00
          </DurationTick>
          <Box position="absolute" top="125px" right="-5px" borderBottomWidth={1} zIndex={0} opacity={0.7} width="5px" />
        </>
        )}
        <Flex justifyContent="space-between" borderBottomWidth={4} minHeight="100px">
          <Box minWidth="250px" />
          <Flex justifyContent="flex-end" overflowX="scroll" pt="100px" mt="-100px">
            {runs.reverse().map((run, i) => (
              <DagRunBar
                key={run.runId}
                run={run}
                index={i}
                totalRuns={runs.length}
                max={max}
                containerRef={containerRef}
              />
            ))}
          </Flex>
        </Flex>
        {renderGroup(groups, 0)}
        <div ref={containerRef} />
      </Box>
    </Box>
  );
};

export default Tree;
