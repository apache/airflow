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
import { Text, Table, Tbody, Tr, Td, Box } from "@chakra-ui/react";

import type { API } from "src/types";

interface Props {
  taskInstance?: API.TaskInstance;
}

const TriggererInfo = ({ taskInstance }: Props) => {
  if (!taskInstance?.trigger || !taskInstance?.triggererJob) return null;

  return (
    <Box mt={3} flexGrow={1}>
      <Text as="strong" mb={3}>
        Triggerer info
      </Text>
      <Table variant="striped" mb={3}>
        <Tbody>
          <Tr>
            <Td>Trigger class</Td>
            <Td>{`${taskInstance?.trigger?.classpath}`}</Td>
          </Tr>
          <Tr>
            <Td>Trigger ID</Td>
            <Td>{`${taskInstance?.trigger?.id}`}</Td>
          </Tr>
          <Tr>
            <Td>Trigger creation time</Td>
            <Td>{`${taskInstance?.trigger?.createdDate}`}</Td>
          </Tr>
          <Tr>
            <Td>Assigned triggerer</Td>
            <Td>{`${taskInstance?.triggererJob?.hostname}`}</Td>
          </Tr>
          <Tr>
            <Td>Latest triggerer heartbeat</Td>
            <Td>{`${taskInstance?.triggererJob?.latestHeartbeat}`}</Td>
          </Tr>
        </Tbody>
      </Table>
    </Box>
  );
};

export default TriggererInfo;
