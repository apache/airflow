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

/* global stateColors, moment */

import React from 'react';
import {
  Flex,
  Box,
  Tooltip,
  Text,
  VStack,
} from '@chakra-ui/react';
import { MdPlayArrow } from 'react-icons/md';

import DagRunTooltip from './Tooltip';
import { callModalDag } from '../../dag';

const DagRunBar = ({
  run, max, index, totalRuns, containerRef,
}) => (
  <Box position="relative">
    <Flex
      height="100px"
      alignItems="flex-end"
      justifyContent="center"
      pb="2px"
      px="3px"
      onClick={() => {
        callModalDag({ execution_date: run.executionDate, dag_id: run.dagId });
      }}
    >
      <Tooltip
        label={<DagRunTooltip dagRun={run} />}
        hasArrow
        portalProps={{ containerRef }}
        placement="top"
        openDelay={100}
      >
        <Flex
          width="10px"
          height={`${(run.duration / max) * 100}px`}
          minHeight="12px"
          backgroundColor={stateColors[run.state]}
          borderRadius={2}
          cursor="pointer"
          pb="2px"
          direction="column"
          justifyContent="flex-end"
          alignItems="center"
          px="1px"
          zIndex={1}
          data-testid="run"
        >
          {run.runType === 'manual' && <MdPlayArrow size="8px" color="white" data-testid="manual-run" />}
        </Flex>
      </Tooltip>
    </Flex>
    {index < totalRuns - 3 && index % 10 === 0 && (
    <VStack position="absolute" top="0" left="-22px" spacing={0}>
      <Text fontSize="10px" color="gray.400" whiteSpace="nowrap" transform="rotate(-30deg) translateX(28px)" mt="-23px !important">
        {moment.utc(run.executionDate).format('MMM DD, HH:mm')}
      </Text>
      <Box borderLeftWidth={1} zIndex={0} opacity={0.7} height="100px" />
    </VStack>
    )}
  </Box>
);

export default DagRunBar;
