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

// Components to customize cell elements in the PipelinesTable

import React from 'react';
import { Link as RouterLink } from 'react-router-dom';
import {
  Tooltip,
  Switch,
  useDisclosure,
  IconButton,
  Link,
  Tag,
} from '@chakra-ui/react';

import TriggerRunModal from 'components/TriggerRunModal';
import { useSaveDag } from 'api';
import { MdPlayArrow } from 'react-icons/md';
import type { DagTag as DagTagType } from 'interfaces';

interface PauseProps {
  dagId: string;
  isPaused: boolean;
  offset?: number;
}

export const PauseToggle: React.FC<PauseProps> = ({ dagId, isPaused, offset = 0 }) => {
  const mutation = useSaveDag(dagId, offset);
  const togglePaused = () => mutation.mutate({ isPaused: !isPaused });

  return (
    <Tooltip
      label={isPaused ? 'Activate DAG' : 'Pause DAG'}
      aria-label={isPaused ? 'Activate DAG' : 'Pause DAG'}
      hasArrow
    >
      {/* span helps tooltip find its position */}
      <span>
        <Switch
          role="switch"
          isChecked={!isPaused}
          onChange={togglePaused}
        />
      </span>
    </Tooltip>
  );
};

export const TriggerDagButton: React.FC<{ dagId: string }> = ({ dagId }) => {
  const { isOpen, onToggle, onClose } = useDisclosure();
  return (
    <>
      <Tooltip
        label="Trigger DAG"
        aria-label="Trigger DAG"
        hasArrow
      >
        <IconButton
          size="sm"
          aria-label="Trigger Dag"
          icon={<MdPlayArrow />}
          onClick={onToggle}
        />
      </Tooltip>
      <TriggerRunModal dagId={dagId} isOpen={isOpen} onClose={onClose} />
    </>
  );
};

export const DagName: React.FC<{ dagId: string }> = ({ dagId }) => (
  <Link
    as={RouterLink}
    to={`/pipelines/${dagId}`}
    fontWeight="bold"
  >
    {dagId}
  </Link>
);

export const DagTag: React.FC<{ tag: DagTagType }> = ({ tag }) => (
  <Tag
    size="sm"
    mt="1"
    ml="1"
    mb="1"
  >
    {tag.name}
  </Tag>
);
