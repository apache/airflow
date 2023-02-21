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

import { useQueueRun } from 'src/api';
import ConfirmDialog from 'src/components/ConfirmDialog';
import { getMetaValue } from 'src/utils';

const canEdit = getMetaValue('can_edit') === 'True';

interface Props {
  dagId: string;
  runId: string;
}

const QueueRun = ({ dagId, runId }: Props) => {
  const [affectedTasks, setAffectedTasks] = useState<string[]>([]);
  const { isOpen, onOpen, onClose } = useDisclosure();
  const { mutateAsync: onQueue, isLoading } = useQueueRun(dagId, runId);

  // Get what the changes will be and show it in a modal
  const onClick = async () => {
    const data = await onQueue({ confirmed: false });
    setAffectedTasks(data);
    onOpen();
  };

  // Confirm changes
  const onConfirm = async () => {
    await onQueue({ confirmed: true });
    setAffectedTasks([]);
    onClose();
  };

  return (
    <>
      <Button
        onClick={onClick}
        isLoading={isLoading}
        ml="5px"
        title="Queue up new tasks to make the DAG run up-to-date with any DAG file changes."
        isDisabled={!canEdit}
      >
        Queue up new tasks
      </Button>
      <ConfirmDialog
        isOpen={isOpen}
        onClose={onClose}
        onConfirm={onConfirm}
        isLoading={isLoading}
        description="Task instances you are about to queue:"
        affectedTasks={affectedTasks}
      />
    </>
  );
};

export default QueueRun;
