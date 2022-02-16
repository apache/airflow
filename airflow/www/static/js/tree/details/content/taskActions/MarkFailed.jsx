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

import { useMarkFailedTask } from '../../../api';

const Run = ({
  dagId,
  runId,
  taskId,
}) => {
  const { isOpen: past, onToggle: onTogglePast } = useDisclosure();
  const { isOpen: future, onToggle: onToggleFuture } = useDisclosure();
  const { isOpen: upstream, onToggle: onToggleUpstream } = useDisclosure();
  const { isOpen: downstream, onToggle: onToggleDownstream } = useDisclosure();

  const { mutate: markFailedMutation, isLoading } = useMarkFailedTask({
    dagId, runId, taskId,
  });

  const markFailed = () => {
    markFailedMutation({
      past,
      future,
      upstream,
      downstream,
    });
  };

  return (
    <Flex justifyContent="space-between" width="100%">
      <ButtonGroup isAttached variant="outline">
        <Button bg={past && 'gray.100'} onClick={onTogglePast}>Past</Button>
        <Button bg={future && 'gray.100'} onClick={onToggleFuture}>Future</Button>
        <Button bg={upstream && 'gray.100'} onClick={onToggleUpstream}>Upstream</Button>
        <Button bg={downstream && 'gray.100'} onClick={onToggleDownstream}>Downstream</Button>
      </ButtonGroup>
      <Button colorScheme="red" onClick={markFailed} isLoading={isLoading}>
        Mark Failed
      </Button>
    </Flex>
  );
};

export default Run;
