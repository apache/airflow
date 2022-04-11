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
  Tooltip,
} from '@chakra-ui/react';

import { useRunTask } from '../../../../api';
import { getMetaValue } from '../../../../../utils';
import { useContainerRef } from '../../../../context/containerRef';

const canRun = getMetaValue('k8s_or_k8scelery_executor') === 'True';

const Run = ({
  dagId,
  runId,
  taskId,
}) => {
  const containerRef = useContainerRef();
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
    });
  };

  return (
    <Flex justifyContent="space-between" width="100%">
      <ButtonGroup isAttached variant="outline" isDisabled={!canRun}>
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
      <Tooltip
        label="Only works with the Celery, CeleryKubernetes or Kubernetes executors"
        shouldWrapChildren // Will show the tooltip even if the button is disabled
        disabled={canRun}
        portalProps={{ containerRef }}
      >
        <Button colorScheme="blue" onClick={onClick} isLoading={isLoading} disabled={!canRun}>
          Run
        </Button>
      </Tooltip>
    </Flex>
  );
};

export default Run;
