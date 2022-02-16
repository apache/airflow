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
  Button,
  Flex,
  ButtonGroup,
  useDisclosure,
} from '@chakra-ui/react';

import { useRunTask } from '../../../api';

const Run = ({
  dagId,
  runId,
  taskId,
}) => {
  const { isOpen: isAllDeps, onToggle: onToggleAllDeps } = useDisclosure();
  const { isOpen: isTaskState, onToggle: onToggleTaskState } = useDisclosure();
  const { isOpen: isTaskDeps, onToggle: onToggleTaskDeps } = useDisclosure();

  const { mutate: onRun } = useRunTask(dagId, runId, taskId);

  return (
    <Flex justifyContent="space-between" width="100%">
      <ButtonGroup isAttached variant="outline">
        <Button bg={isAllDeps && 'gray.100'} onClick={onToggleAllDeps}>Ignore All Deps</Button>
        <Button bg={isTaskState && 'gray.100'} onClick={onToggleTaskState}>Ignore Task State</Button>
        <Button bg={isTaskDeps && 'gray.100'} onClick={onToggleTaskDeps}>Ignore Task Deps</Button>
      </ButtonGroup>
      <Button colorScheme="blue" onClick={onRun}>
        Run
      </Button>
    </Flex>
  );
};

export default Run;
