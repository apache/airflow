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

import ConfirmDialog from '../../ConfirmDialog';
import ActionButton from './ActionButton';
import { useMarkSuccessTask, useConfirmMarkTask } from '../../../../api';
import { getMetaValue } from '../../../../utils';

const canEdit = getMetaValue('can_edit') === 'True';

const MarkSuccess = ({
  dagId, runId, taskId, mapIndexes,
}) => {
  const [affectedTasks, setAffectedTasks] = useState([]);

  // Options check/unchecked
  const [past, setPast] = useState(false);
  const onTogglePast = () => setPast(!past);

  const [future, setFuture] = useState(false);
  const onToggleFuture = () => setFuture(!future);

  const [upstream, setUpstream] = useState(false);
  const onToggleUpstream = () => setUpstream(!upstream);

  const [downstream, setDownstream] = useState(false);
  const onToggleDownstream = () => setDownstream(!downstream);

  // Confirm dialog open/close
  const { isOpen, onOpen, onClose } = useDisclosure();

  const { mutateAsync: markSuccessMutation, isLoading: isMarkLoading } = useMarkSuccessTask({
    dagId, runId, taskId,
  });
  const { mutateAsync: confirmChangeMutation, isLoading: isConfirmLoading } = useConfirmMarkTask({
    dagId, runId, taskId, state: 'success',
  });

  const onClick = async () => {
    const data = await confirmChangeMutation({
      past,
      future,
      upstream,
      downstream,
      mapIndexes,
    });
    setAffectedTasks(data);
    onOpen();
  };

  const onConfirm = async () => {
    await markSuccessMutation({
      past,
      future,
      upstream,
      downstream,
      mapIndexes,
    });
    setAffectedTasks([]);
    onClose();
  };

  const isLoading = isMarkLoading || isConfirmLoading;

  return (
    <Flex justifyContent="space-between" width="100%">
      <ButtonGroup isAttached variant="outline" isDisabled={!canEdit}>
        <ActionButton bg={past && 'gray.100'} onClick={onTogglePast} name="Past" />
        <ActionButton bg={future && 'gray.100'} onClick={onToggleFuture} name="Future" />
        <ActionButton bg={upstream && 'gray.100'} onClick={onToggleUpstream} name="Upstream" />
        <ActionButton bg={downstream && 'gray.100'} onClick={onToggleDownstream} name="Downstream" />
      </ButtonGroup>
      <Button colorScheme="green" onClick={onClick} isLoading={isLoading} isDisabled={!canEdit}>
        Mark Success
      </Button>
      <ConfirmDialog
        isOpen={isOpen}
        onClose={onClose}
        onConfirm={onConfirm}
        isLoading={isLoading}
        description="Task instances you are about to mark as success:"
        body={affectedTasks}
      />
    </Flex>
  );
};

export default MarkSuccess;
