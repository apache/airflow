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
import { Link as RouterLink } from 'react-router-dom';
import {
  Link,
  Alert,
  AlertIcon,
  Flex,
  Table,
  Thead,
  Tbody,
  Tr,
  Th,
  Td,
  useColorModeValue,
} from '@chakra-ui/react';

import AppContainer from 'components/AppContainer';
import { defaultDags } from 'api/defaults';
import { useDags } from 'api';
import type { Dag } from 'interfaces';

const Pipelines: React.FC = () => {
  const { data: { dags } = defaultDags, isLoading, error } = useDags();
  const oddColor = useColorModeValue('gray.50', 'gray.900');
  const evenColor = useColorModeValue('gray.100', 'gray.700');

  return (
    <AppContainer>
      {error && (
        <Alert status="error" my="4" key={error.message}>
          <AlertIcon />
          {error.message}
        </Alert>
      )}
      <Table size="sm">
        <Thead position="sticky" top={0}>
          <Tr
            borderBottomWidth="1px"
            textAlign="left"
          >
            <Th>DAG ID</Th>
          </Tr>
        </Thead>
        <Tbody>
          {isLoading && (
          <Tr>
            <Td colSpan={2}>Loading…</Td>
          </Tr>
          )}
          {(!isLoading && !dags.length) && (
          <Tr>
            <Td colSpan={2}>No Pipelines found.</Td>
          </Tr>
          )}
          {dags.map((dag: Dag) => (
            <Tr
              key={dag.dagId}
              _odd={{
                backgroundColor: oddColor,
              }}
              _hover={{
                backgroundColor: evenColor,
              }}
            >
              <Td>
                <Flex alignItems="center">
                  <Link
                    as={RouterLink}
                    to={`/pipeline/${dag.dagId}`}
                    fontWeight="bold"
                  >
                    {dag.dagId}
                  </Link>
                </Flex>
              </Td>
            </Tr>
          ))}
        </Tbody>
      </Table>
    </AppContainer>
  );
};

export default Pipelines;
