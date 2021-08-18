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
  <Text fontSize={10} color="gray.400" position="absolute" left="calc(100% + 5px)" whiteSpace="nowrap" {...rest}>
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

    return (
      <Box backgroundColor={`rgba(203, 213, 224, ${0.25 * level})`}>
        <Flex
          justifyContent="space-between"
          alignItems="center"
          width="100%"
          borderBottomWidth={2}
          borderBottomColor={level ? 'white' : 'gray.200'}
          backgroundColor={isHover && 'rgba(113, 128, 150, 0.2)'}
          onMouseEnter={() => setHover(true)}
          onMouseLeave={() => setHover(false)}
        >
          <Flex
            as={isGroup ? 'button' : 'div'}
            onClick={() => isGroup && onToggle()}
            color={(level > 3 || (isHover && level > 1)) && 'white'}
            minWidth="200px"
          >
            <Text display="inline" fontSize={12} ml={`${level * 10 + 10}px`} maxWidth="500px">{group.id.replace(`${prevGroup}.`, '')}</Text>
            {isGroup && (
              isOpen ? <FiChevronDown value={{ style: { height: '17px' } }} /> : <FiChevronUp value={{ style: { height: '17px' } }} />
            )}
          </Flex>
          <Flex justifyContent="flex-end" overflowX="scroll" overflowY="visible">
            {group.instances.map((instance) => (
              <StatusBox
                key={`${instance.runId}-${instance.taskId}`}
                instance={instance}
                containerRef={containerRef}
                group={group}
              />
            ))}
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
        <Text transform="rotate(-90deg)" position="absolute" left="-20px" top="45px">Dag Runs</Text>
        <Text transform="rotate(-90deg)" position="absolute" left="-6px" top="135px">Tasks</Text>
        {!!runs.length && (
        <>
          <DurationTick top={-1}>Duration</DurationTick>
          <DurationTick top={6}>
            -
            {' '}
            {formatDuration(max)}
          </DurationTick>
          <Box position="absolute" top="22px" borderBottomWidth={1} zIndex={0} opacity={0.7} width="calc(100% - 25px)" />
          <DurationTick top={65}>
            -
            {' '}
            {formatDuration(max / 2)}
          </DurationTick>
          <Box position="absolute" top="72px" borderBottomWidth={1} zIndex={0} opacity={0.7} width="calc(100% - 25px)" />
          <DurationTick top={116}>
            - 00:00:00
          </DurationTick>
        </>
        )}
        <Flex justifyContent="space-between" borderBottomWidth={2} minHeight="100px">
          <Box minWidth="200px" />
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
