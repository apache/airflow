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
import { Flex, Heading, Button } from "@chakra-ui/react";
import { useState } from "react";
import { useHotkeys } from "react-hotkeys-hook";

import type { TaskInstanceCollectionResponse } from "openapi/requests/types.gen";
import { Tooltip } from "src/components/ui";
import { useConfig } from "src/queries/useConfig";

import { TaskLogPreview } from "./TaskLogPreview";

const FailedLogs = ({
  failedTasks,
}: {
  readonly failedTasks: TaskInstanceCollectionResponse | undefined;
}) => {
  const defaultWrap = Boolean(useConfig("default_wrap"));
  const [wrap, setWrap] = useState(defaultWrap);

  const taskLogs = failedTasks?.task_instances.slice(0, 5);

  const toggleWrap = () => setWrap(!wrap);

  useHotkeys("w", toggleWrap);

  if (taskLogs === undefined || taskLogs.length <= 0) {
    return undefined;
  }

  return (
    <Flex flexDirection="column" gap={3}>
      <Flex alignItems="center" justifyContent="space-between">
        <Heading size="md">Recent Failed Task Logs</Heading>
        <Tooltip closeDelay={100} content="Press w to toggle wrap" openDelay={100}>
          <Button
            aria-label={wrap ? "Unwrap" : "Wrap"}
            bg="bg.panel"
            fontSize="sm"
            onClick={toggleWrap}
            size="sm"
            variant="outline"
          >
            {wrap ? "Unwrap" : "Wrap"}
          </Button>
        </Tooltip>
      </Flex>
      {taskLogs.map((taskInstance) => (
        <TaskLogPreview key={taskInstance.id} taskInstance={taskInstance} wrap={wrap} />
      ))}
    </Flex>
  );
};

export default FailedLogs;
