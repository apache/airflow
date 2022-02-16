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

import Header from './Header';
import TaskInstanceContent from './content/TaskInstance';
import DagRunContent from './content/DagRun';
import DagContent from './content/Dag';
import useTreeData from '../useTreeData';

const Details = ({
  selected,
  onSelect,
}) => {
  const { data: { dagRuns = [] } } = useTreeData();
  return (
    <Flex borderLeftWidth="1px" flexDirection="column" p={3} flexGrow={1} maxWidth="600px">
      <Header selected={selected} onSelect={onSelect} dagRuns={dagRuns} />
      <Divider my={2} />
      <Box minWidth="500px">
        {/* TODO: get full instance data from the API */}
        {!selected.runId && !selected.taskId && <DagContent />}
        {selected.runId && !selected.taskId && <DagRunContent dagRun={selected.dagRun} />}
        {selected.taskId && (
        <TaskInstanceContent
          instance={selected.instance}
          task={selected.task}
        />
        )}
      </Box>
    </Flex>
  );
};

export default Details;
