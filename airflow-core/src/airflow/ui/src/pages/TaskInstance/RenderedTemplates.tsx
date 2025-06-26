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
import { Box, HStack, Table, Code } from "@chakra-ui/react";
import { useParams } from "react-router-dom";

import { useTaskInstanceServiceGetMappedTaskInstance } from "openapi/queries";
import { ClipboardRoot, ClipboardIconButton } from "src/components/ui";

export const RenderedTemplates = () => {
  const { dagId = "", mapIndex = "-1", runId = "", taskId = "" } = useParams();

  const { data: taskInstance } = useTaskInstanceServiceGetMappedTaskInstance({
    dagId,
    dagRunId: runId,
    mapIndex: parseInt(mapIndex, 10),
    taskId,
  });

  return (
    <Box p={2}>
      <Table.Root striped>
        <Table.Body>
          {Object.entries(taskInstance?.rendered_fields ?? {}).map(([key, value]) => {
            if (value !== null && value !== undefined) {
              const renderedValue = JSON.stringify(value);

              return (
                <Table.Row key={key}>
                  <Table.Cell>{key}</Table.Cell>
                  <Table.Cell>
                    <HStack>
                      <Code>{renderedValue}</Code>
                      <ClipboardRoot value={renderedValue}>
                        <ClipboardIconButton />
                      </ClipboardRoot>
                    </HStack>
                  </Table.Cell>
                </Table.Row>
              );
            }

            return undefined;
          })}
        </Table.Body>
      </Table.Root>
    </Box>
  );
};
