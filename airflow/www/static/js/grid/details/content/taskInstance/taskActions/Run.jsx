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

import React, { useState } from 'react';
import {
  Button,
  Flex,
  ButtonGroup,
} from '@chakra-ui/react';

import { useRunTask } from '../../../../api';
import { getMetaValue } from '../../../../../utils';

const canEdit = getMetaValue('can_edit') === 'True';

const Run = ({
  dagId,
  runId,
  taskId,
  mapIndexes,
}) => {
  const [ignoreAllDeps, setIgnoreAllDeps] = useState(false);
  const onToggleAllDeps = () => setIgnoreAllDeps(!ignoreAllDeps);

  const [ignoreTaskState, setIgnoreTaskState] = useState(false);
  const onToggleTaskState = () => setIgnoreTaskState(!ignoreTaskState);

  const [ignoreTaskDeps, setIgnoreTaskDeps] = useState(false);
  const onToggleTaskDeps = () => setIgnoreTaskDeps(!ignoreTaskDeps);

  const { mutate: onRun, isLoading } = useRunTask(dagId, runId, taskId);

  const onClick = () => {
    onRun({
      ignoreAllDeps,
      ignoreTaskState,
      ignoreTaskDeps,
      mapIndexes,
    });
  };

  return (
    <Flex justifyContent="space-between" width="100%">
      <ButtonGroup isAttached variant="outline" isDisabled={!canEdit}>
        <Button
          bg={ignoreAllDeps && 'gray.100'}
          onClick={onToggleAllDeps}
          title="Ignores all non-critical dependencies, including task state and task_deps"
        >
          Ignore All Deps
        </Button>
        <Button
          bg={ignoreTaskState && 'gray.100'}
          onClick={onToggleTaskState}
          title="Ignore previous success/failure"
        >
          Ignore Task State
        </Button>
        <Button
          bg={ignoreTaskDeps && 'gray.100'}
          onClick={onToggleTaskDeps}
          title="Disregard the task-specific dependencies, e.g. status of upstream task instances and depends_on_past"
        >
          Ignore Task Deps
        </Button>
      </ButtonGroup>
      <Button colorScheme="blue" onClick={onClick} isLoading={isLoading} isDisabled={!canEdit}>
        Run
      </Button>
    </Flex>
  );
};

export default Run;
