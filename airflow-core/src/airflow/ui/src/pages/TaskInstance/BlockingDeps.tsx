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
import { Box, Table, Heading } from "@chakra-ui/react";

import { useTaskInstanceServiceGetTaskInstanceDependencies } from "openapi/queries";
import type { TaskInstanceResponse } from "openapi/requests/types.gen";

export const BlockingDeps = ({ taskInstance }: { readonly taskInstance: TaskInstanceResponse }) => {
  const { data } = useTaskInstanceServiceGetTaskInstanceDependencies({
    dagId: taskInstance.dag_id,
    dagRunId: taskInstance.dag_run_id,
    mapIndex: taskInstance.map_index,
    taskId: taskInstance.task_id,
  });

  if (data === undefined || data.dependencies.length < 1) {
    return undefined;
  }

  return (
    <Box flexGrow={1} mt={3}>
      <Heading py={2} size="sm">
        Dependencies Blocking Task From Getting Scheduled
      </Heading>
      <Table.Root striped>
        <Table.Body>
          <Table.Row>
            <Table.ColumnHeader>Dependency</Table.ColumnHeader>
            <Table.ColumnHeader>Reason</Table.ColumnHeader>
          </Table.Row>
          {data.dependencies.map((dep) => (
            <Table.Row key={dep.name}>
              <Table.Cell>{dep.name}</Table.Cell>
              <Table.Cell>{dep.reason}</Table.Cell>
            </Table.Row>
          ))}
        </Table.Body>
      </Table.Root>
    </Box>
  );
};
