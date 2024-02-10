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

import React, { useRef } from "react";
import type { Dag, DagRun, TaskInstance } from "src/types";
import {
  Table,
  Text,
  Thead,
  Tbody,
  Tr,
  Td,
  Spinner,
  Alert,
  AlertIcon,
  Box,
} from "@chakra-ui/react";
import { useTaskXcomCollection } from "src/api";
import { useOffsetTop } from "src/utils";
import XcomEntry from "./XcomEntry";

interface Props {
  dagId: Dag["id"];
  dagRunId: DagRun["runId"];
  taskId: TaskInstance["taskId"];
  mapIndex?: TaskInstance["mapIndex"];
  tryNumber: TaskInstance["tryNumber"];
}

const XcomCollection = ({
  dagId,
  dagRunId,
  taskId,
  mapIndex,
  tryNumber,
}: Props) => {
  const taskXcomRef = useRef<HTMLDivElement>(null);
  const offsetTop = useOffsetTop(taskXcomRef);

  const {
    data: xcomCollection,
    isLoading,
    error,
  } = useTaskXcomCollection({
    dagId,
    dagRunId,
    taskId,
    mapIndex,
    tryNumber: tryNumber || 1,
  });

  return (
    <Box
      ref={taskXcomRef}
      height="100%"
      maxHeight={`calc(100% - ${offsetTop}px)`}
      overflowY="auto"
    >
      {isLoading && <Spinner size="xl" thickness="4px" speed="0.65s" />}
      {!!error && (
        <Alert status="error" marginBottom="10px">
          <AlertIcon />
          An error occurred while fetching task XCom.
        </Alert>
      )}
      {xcomCollection &&
        (xcomCollection.totalEntries === 0 ? (
          <Text>No XCom</Text>
        ) : (
          <Table variant="striped">
            <Thead>
              <Tr>
                <Td>
                  <Text as="b">Key</Text>
                </Td>
                <Td>
                  <Text as="b">Value</Text>
                </Td>
              </Tr>
            </Thead>
            <Tbody>
              {xcomCollection.xcomEntries?.map((xcomEntry) => (
                <XcomEntry
                  key={xcomEntry.key}
                  dagId={dagId}
                  dagRunId={dagRunId}
                  taskId={taskId}
                  mapIndex={mapIndex}
                  xcomKey={xcomEntry.key || ""}
                  tryNumber={tryNumber}
                />
              ))}
            </Tbody>
          </Table>
        ))}
    </Box>
  );
};

export default XcomCollection;
