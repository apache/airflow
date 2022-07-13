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
import { Button, useDisclosure } from '@chakra-ui/react';

import { useMarkSuccessRun } from '../../../api';
import ConfirmDialog from '../ConfirmDialog';
import { getMetaValue } from '../../../utils';

const canEdit = getMetaValue('can_edit') === 'True';

const MarkSuccessRun = ({ dagId, runId }) => {
  const [affectedTasks, setAffectedTasks] = useState([]);
  const { isOpen, onOpen, onClose } = useDisclosure();
  const { mutateAsync: markSuccess, isLoading } = useMarkSuccessRun(dagId, runId);

  const onClick = async () => {
    const data = await markSuccess({ confirmed: false });
    setAffectedTasks(data);
    onOpen();
  };

  const onConfirm = async () => {
    await markSuccess({ confirmed: true });
    setAffectedTasks([]);
    onClose();
  };

  return (
    <>
      <Button onClick={onClick} colorScheme="green" isLoading={isLoading} isDisabled={!canEdit}>Mark Success</Button>
      <ConfirmDialog
        isOpen={isOpen}
        onClose={onClose}
        onConfirm={onConfirm}
        isLoading={isLoading}
        description="Task instances you are about to mark as success:"
        body={affectedTasks}
      />
    </>
  );
};

export default MarkSuccessRun;
