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
  useDisclosure,
} from '@chakra-ui/react';

import ActionButton from './ActionButton';
import ConfirmDialog from '../../ConfirmDialog';
import { useClearTask } from '../../../../api';
import { getMetaValue } from '../../../../../utils';

const canEdit = getMetaValue('can_edit') === 'True';

const Run = ({
  dagId,
  runId,
  taskId,
  executionDate,
  mapIndexes,
}) => {
  const [affectedTasks, setAffectedTasks] = useState([]);

  // Options check/unchecked
  const [past, setPast] = useState(false);
  const onTogglePast = () => setPast(!past);

  const [future, setFuture] = useState(false);
  const onToggleFuture = () => setFuture(!future);

  const [upstream, setUpstream] = useState(false);
  const onToggleUpstream = () => setUpstream(!upstream);

  const [downstream, setDownstream] = useState(true);
  const onToggleDownstream = () => setDownstream(!downstream);

  const [recursive, setRecursive] = useState(true);
  const onToggleRecursive = () => setRecursive(!recursive);

  const [failed, setFailed] = useState(false);
  const onToggleFailed = () => setFailed(!failed);

  // Confirm dialog open/close
  const { isOpen, onOpen, onClose } = useDisclosure();

  const { mutateAsync: clearTask, isLoading } = useClearTask({
    dagId, runId, taskId, executionDate,
  });

  const onClick = async () => {
    const data = await clearTask({
      past,
      future,
      upstream,
      downstream,
      recursive,
      failed,
      confirmed: false,
      mapIndexes,
    });
    setAffectedTasks(data);
    onOpen();
  };

  const onConfirm = async () => {
    await clearTask({
      past,
      future,
      upstream,
      downstream,
      recursive,
      failed,
      confirmed: true,
      mapIndexes,
    });
    setAffectedTasks([]);
    onClose();
  };

  return (
    <Flex justifyContent="space-between" width="100%">
      <ButtonGroup isAttached variant="outline" isDisabled={!canEdit}>
        <ActionButton bg={past && 'gray.100'} onClick={onTogglePast} name="Past" />
        <ActionButton bg={future && 'gray.100'} onClick={onToggleFuture} name="Future" />
        <ActionButton bg={upstream && 'gray.100'} onClick={onToggleUpstream} name="Upstream" />
        <ActionButton bg={downstream && 'gray.100'} onClick={onToggleDownstream} name="Downstream" />
        <ActionButton bg={recursive && 'gray.100'} onClick={onToggleRecursive} name="Recursive" />
        <ActionButton bg={failed && 'gray.100'} onClick={onToggleFailed} name="Failed" />
      </ButtonGroup>
      <Button
        colorScheme="blue"
        onClick={onClick}
        isLoading={isLoading}
        isDisabled={!canEdit}
        title="Clearing deletes the previous state of the task instance, allowing it to get re-triggered by the scheduler or a backfill command"
      >
        Clear
      </Button>
      <ConfirmDialog
        isOpen={isOpen}
        onClose={onClose}
        onConfirm={onConfirm}
        isLoading={isLoading}
        description="Task instances you are about to clear:"
        body={affectedTasks}
      />
    </Flex>
  );
};

export default Run;
