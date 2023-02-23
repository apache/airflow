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
  Box,
  VStack,
  Divider,
  StackDivider,
  Text,
} from '@chakra-ui/react';

import type { CommonActionProps } from './types';
import ClearAction from './Clear';
import MarkFailedAction from './MarkFailed';
import MarkSuccessAction from './MarkSuccess';

type Props = {
  title: string;
} & CommonActionProps;

const TaskActions = ({
  title, runId, taskId, dagId, executionDate, mapIndexes, isGroup,
}: Props) => (
  <Box my={3}>
    <Text as="strong" size="lg">{title}</Text>
    <Divider my={2} />
    {/* For now only ClearAction is supported for groups */}
    {isGroup ? (
      <ClearAction
        runId={runId}
        taskId={taskId}
        dagId={dagId}
        executionDate={executionDate}
        mapIndexes={mapIndexes}
        isGroup={isGroup}
      />
    ) : (
      <VStack justifyContent="center" divider={<StackDivider my={3} />}>
        <ClearAction
          runId={runId}
          taskId={taskId}
          dagId={dagId}
          executionDate={executionDate}
          mapIndexes={mapIndexes}
          isGroup={!!isGroup}
        />
        <MarkFailedAction
          runId={runId}
          taskId={taskId}
          dagId={dagId}
          mapIndexes={mapIndexes}
        />
        <MarkSuccessAction
          runId={runId}
          taskId={taskId}
          dagId={dagId}
          mapIndexes={mapIndexes}
        />
      </VStack>
    )}
    <Divider my={2} />
  </Box>
);

export default TaskActions;
