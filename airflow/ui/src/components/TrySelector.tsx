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
import { Box, Flex, Heading } from "@chakra-ui/react";

import { useTaskInstanceServiceGetTaskInstanceTries } from "openapi/queries";
import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { Button, Status } from "src/components/ui";

type Props = {
  readonly onSelectTryNumber?: (tryNumber: number) => void;
  readonly selectedTryNumber?: number;
  readonly taskInstance?: TaskInstanceResponse;
};

export const TrySelector = ({
  onSelectTryNumber,
  selectedTryNumber,
  taskInstance,
}: Props) => {
  const {
    dag_id: dagId,
    dag_run_id: dagRunId,
    map_index: mapIndex,
    task_id: taskId,
    try_number: finalTryNumber,
  } = taskInstance;

  const { data: taskInstanceTries } =
    useTaskInstanceServiceGetTaskInstanceTries({
      dagId,
      dagRunId,
      mapIndex,
      taskId,
    });

  return (
    <Box my={3}>
      <Heading size="md">
        <strong>Task Tries</strong>
      </Heading>
      <Flex flexWrap="wrap" my={1}>
        {taskInstanceTries?.task_instances.map((ti) => (
          <Button
            colorPalette="blue"
            key={ti.try_number}
            variant={selectedTryNumber === ti.try_number ? "solid" : "ghost"}
          >
            {ti.try_number}
            <Status ml={2} state={ti.state} />
          </Button>
        ))}
      </Flex>
    </Box>
  );
};
