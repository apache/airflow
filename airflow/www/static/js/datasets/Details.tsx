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
  Box, Heading, Flex, Text, Spinner, Button, Link,
} from '@chakra-ui/react';

import { useDataset } from 'src/api';
import { ClipboardButton } from 'src/components/Clipboard';
import InfoTooltip from 'src/components/InfoTooltip';
import { getMetaValue } from 'src/utils';
import Events from './DatasetEvents';

interface Props {
  datasetUri: string;
  onBack: () => void;
}

const gridUrl = getMetaValue('grid_url');

const DatasetDetails = ({ datasetUri, onBack }: Props) => {
  const { data: dataset, isLoading } = useDataset({ datasetUri });

  return (
    <Box mt={[6, 3]} maxWidth="1500px">
      <Button onClick={onBack}>See all datasets</Button>
      {isLoading && <Spinner display="block" />}
      <Box>
        <Heading my={2} fontWeight="normal">
          Dataset:
          {' '}
          {datasetUri}
          <ClipboardButton value={datasetUri} iconOnly ml={2} />
        </Heading>
        {dataset?.producingTasks && !!dataset.producingTasks.length && (
        <Box mb={2}>
          <Flex alignItems="center">
            <Heading size="md" fontWeight="normal">Producing Tasks</Heading>
            <InfoTooltip label="Tasks that will update this dataset." size={14} />
          </Flex>
          {dataset.producingTasks.map(({ dagId, taskId }) => (
            <Link
              key={`${dagId}.${taskId}`}
              color="blue.600"
              href={dagId ? gridUrl?.replace('__DAG_ID__', dagId) : ''}
              display="block"
            >
              {`${dagId}.${taskId}`}
            </Link>
          ))}
        </Box>
        )}
        {dataset?.consumingDags && !!dataset.consumingDags.length && (
        <Box>
          <Flex alignItems="center">
            <Heading size="md" fontWeight="normal">Consuming DAGs</Heading>
            <InfoTooltip label="DAGs that depend on this dataset updating to trigger a run." size={14} />
          </Flex>
          {dataset.consumingDags.map(({ dagId }) => (
            <Link
              key={dagId}
              color="blue.600"
              href={dagId ? gridUrl?.replace('__DAG_ID__', dagId) : ''}
              display="block"
            >
              {dagId}
            </Link>
          ))}
        </Box>
        )}
      </Box>
      <Box>
        <Heading size="lg" mt={3} mb={2} fontWeight="normal">History</Heading>
        <Text>Whenever a DAG has updated this dataset.</Text>
      </Box>
      {dataset?.id && (
        <Events datasetId={dataset.id} />
      )}
    </Box>
  );
};

export default DatasetDetails;
