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

import React from "react";
import { Box, Spinner, Text, Table, Tbody, Tr, Th, Td } from "@chakra-ui/react";

import { useTaskFailedDependency } from "src/api";
import ErrorAlert from "src/components/ErrorAlert";

interface Props {
  dagId: string;
  runId: string;
  taskId: string;
  mapIndex?: number;
}

const TaskFailedDependency = ({ dagId, runId, taskId, mapIndex }: Props) => {
  const {
    data: dependencies,
    isLoading,
    error,
  } = useTaskFailedDependency({
    dagId,
    taskId,
    runId,
    mapIndex,
  });

  return (
    <Box mt={3} flexGrow={1}>
      <Text as="strong" mb={3}>
        Task Failed Dependencies
      </Text>
      <br />

      {isLoading && <Spinner size="md" thickness="4px" speed="0.65s" />}
      <ErrorAlert error={error} />
      {dependencies && (
        <Box mt={3}>
          <Text>Dependencies Blocking Task From Getting Scheduled</Text>
          <Table variant="striped">
            <Tbody>
              <Tr>
                <Th>Dependency</Th>
                <Th>Reason</Th>
              </Tr>
              {dependencies.dependencies?.map((dep) => (
                <Tr key={dep.name}>
                  <Td>{dep.name}</Td>
                  <Td>{dep.reason}</Td>
                </Tr>
              ))}
            </Tbody>
          </Table>
        </Box>
      )}
    </Box>
  );
};

export default TaskFailedDependency;
