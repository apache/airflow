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

import React from 'react';
import {
  Flex,
  Box,
  Divider,
} from '@chakra-ui/react';

import useSelection from 'src/dag/useSelection';

import Header from './Header';
import TaskInstanceContent from './taskInstance';
import DagRunContent from './dagRun';
import DagContent from './Dag';

const Details = () => {
  const { selected: { runId, taskId, mapIndex }, onSelect } = useSelection();
  return (
    <Flex flexDirection="column" pl={3} mr={3} flexGrow={1} maxWidth="750px">
      <Header />
      <Divider my={2} />
      <Box minWidth="750px">
        {!runId && !taskId && <DagContent />}
        {runId && !taskId && (
          <DagRunContent runId={runId} />
        )}
        {taskId && runId && (
        <TaskInstanceContent
          runId={runId}
          taskId={taskId}
          mapIndex={mapIndex === null ? undefined : mapIndex}
          onSelect={onSelect}
        />
        )}
      </Box>
    </Flex>
  );
};

export default Details;
