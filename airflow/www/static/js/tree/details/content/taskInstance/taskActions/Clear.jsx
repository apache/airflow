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
import { useClearTask, useConfirmClearTask } from '../../../../api';

const Run = ({
  dagId,
  runId,
  taskId,
  executionDate,
}) => {
  const [affectedTasks, setAffectedTasks] = useState([]);

  // Options check/unchecked
  const { isOpen: past, onToggle: onTogglePast } = useDisclosure();
  const { isOpen: future, onToggle: onToggleFuture } = useDisclosure();
  const { isOpen: upstream, onToggle: onToggleUpstream } = useDisclosure();
  const {
    isOpen: downstream, onToggle: onToggleDownstream,
  } = useDisclosure({ defaultIsOpen: true });
  const {
    isOpen: recursive, onToggle: onToggleRecursive,
  } = useDisclosure({ defaultIsOpen: true });
  const { isOpen: failed, onToggle: onToggleFailed } = useDisclosure();

  // Confirm dialog open/close
  const { isOpen, onOpen, onClose } = useDisclosure();

  const { mutateAsync: clearTaskMutation, isLoading: isClearLoading } = useClearTask({
    dagId, runId, taskId, executionDate,
  });
  const {
    mutateAsync: confirmChangeMutation, isLoading: isConfirmLoading,
  } = useConfirmClearTask({
    dagId, runId, taskId, executionDate,
  });

  const onClick = async () => {
    try {
      const data = await confirmChangeMutation({
        past,
        future,
        upstream,
        downstream,
        recursive,
        failed,
      });
      setAffectedTasks(data);
      onOpen();
    } catch (e) {
      console.error(e);
    }
  };

  const onConfirm = async () => {
    try {
      await clearTaskMutation({
        past,
        future,
        upstream,
        downstream,
        recursive,
        failed,
      });
      setAffectedTasks([]);
      onClose();
    } catch (e) {
      console.error(e);
    }
  };

  return (
    <Flex justifyContent="space-between" width="100%">
      <ButtonGroup isAttached variant="outline">
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
        isLoading={isClearLoading || isConfirmLoading}
        title="Clearing deletes the previous state of the task instance, allowing it to get re-triggered by the scheduler or a backfill command"
      >
        Clear
      </Button>
      <ConfirmDialog
        isOpen={isOpen}
        onClose={onClose}
        onConfirm={onConfirm}
        title="Clear and Re-Trigger Tasks"
        body={affectedTasks}
      />
    </Flex>
  );
};

export default Run;
