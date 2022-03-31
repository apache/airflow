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

/* global moment */

import React from 'react';
import {
  Table,
  Tbody,
  Tr,
  Td,
  Tag,
  Text,
  Code,
  Link,
  Button,
  Flex,
} from '@chakra-ui/react';

import { formatDuration } from '../../../datetime_utils';
import { getMetaValue } from '../../../utils';
import { useDag, useTasks } from '../../api';
import Time from '../../Time';

const dagId = getMetaValue('dag_id');
const dagDetailsUrl = getMetaValue('dag_details_url');

const Dag = () => {
  const { data: dag } = useDag(dagId);
  const { data: taskData } = useTasks(dagId);
  if (!dag || !taskData) return null;
  const { tasks = [], totalEntries = '' } = taskData;
  const {
    description, tags, fileloc, owners, catchup, startDate, timezone, dagRunTimeout,
  } = dag;

  // Build a key/value object of operator counts, the name is hidden inside of t.classRef.className
  const operators = {};
  tasks.forEach((t) => {
    if (!operators[t.classRef.className]) {
      operators[t.classRef.className] = 1;
    } else {
      operators[t.classRef.className] += 1;
    }
  });

  return (
    <>
      <Button as={Link} mb={2} variant="ghost" colorScheme="blue" href={dagDetailsUrl}>
        DAG Details
      </Button>
      <Table variant="striped">
        <Tbody>
          {description && (
          <Tr>
            <Td>Description</Td>
            <Td>{description}</Td>
          </Tr>
          )}
          <Tr>
            <Td>Start Date</Td>
            <Td>
              <Time dateTime={startDate} />
            </Td>
          </Tr>
          <Tr>
            <Td>Total Tasks</Td>
            <Td>{totalEntries}</Td>
          </Tr>
          {Object.entries(operators).map(([key, value]) => (
            <Tr key={key}>
              <Td>
                {key}
                {value > 1 && 's'}
              </Td>
              <Td>{value}</Td>
            </Tr>
          ))}
          {!!tags.length && (
          <Tr>
            <Td>Tags</Td>
            <Td>
              <Flex flexWrap="wrap">
                {tags.map((tag) => (
                  <Link key={tag.name} href={`/home?tags=${tag.name}`} mr={1}>
                    <Tag colorScheme="blue" size="lg">{tag.name}</Tag>
                  </Link>
                ))}
              </Flex>
            </Td>
          </Tr>
          )}
          <Tr>
            <Td>Catchup</Td>
            <Td>{catchup ? 'True' : 'False'}</Td>
          </Tr>
          <Tr>
            <Td>Owners</Td>
            <Td>{owners.map((o) => <Text key={o} mr={1}>{o}</Text>)}</Td>
          </Tr>
          <Tr>
            <Td>Relative File Location</Td>
            <Td><Code colorScheme="blackAlpha" maxWidth="250px">{fileloc}</Code></Td>
          </Tr>
          {dagRunTimeout && (
          <Tr>
            <Td>DAG Run Timeout</Td>
            <Td>{formatDuration(moment.duration(dagRunTimeout.days, 'd').add(dagRunTimeout.seconds, 's'))}</Td>
          </Tr>
          )}
          <Tr>
            <Td>DAG Timezone</Td>
            <Td>{timezone}</Td>
          </Tr>
        </Tbody>
      </Table>
    </>
  );
};

export default Dag;
