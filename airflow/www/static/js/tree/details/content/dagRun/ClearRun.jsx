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

import { useClearRun } from '../../../api';
import ConfirmDialog from '../ConfirmDialog';

const ClearRun = ({ dagId, runId }) => {
  const [affectedTasks, setAffectedTasks] = useState([]);
  const { isOpen, onOpen, onClose } = useDisclosure();
  const { mutateAsync: onClear, isLoading } = useClearRun(dagId, runId);

  const onClick = async () => {
    try {
      const data = await onClear({ confirmed: false });
      setAffectedTasks(data);
      onOpen();
    } catch (e) {
      console.error(e);
    }
  };

  const onConfirm = async () => {
    try {
      await onClear({ confirmed: true });
      setAffectedTasks([]);
      onClose();
    } catch (e) {
      console.error(e);
    }
  };

  return (
    <>
      <Button onClick={onClick} isLoading={isLoading}>Clear existing tasks</Button>
      <ConfirmDialog
        isOpen={isOpen}
        onClose={onClose}
        onConfirm={onConfirm}
        description="Task instances you are about to clear:"
        body={affectedTasks}
      />
    </>
  );
};

export default ClearRun;
