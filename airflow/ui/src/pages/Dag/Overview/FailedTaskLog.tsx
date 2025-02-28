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
import { Box, Flex, Link } from "@chakra-ui/react";
import { Link as RouterLink } from "react-router-dom";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { ClearTaskInstanceButton } from "src/components/Clear";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { TaskLogContent } from "src/pages/TaskInstance/Logs/TaskLogContent";
import { useConfig } from "src/queries/useConfig";
import { useLogs } from "src/queries/useLogs";
import { getTaskInstanceLink } from "src/utils/links";

export const FailedTaskLog = ({ taskInstance }: { readonly taskInstance: TaskInstanceResponse }) => {
  const defaultWrap = Boolean(useConfig("default_wrap"));

  const { data, error, isLoading } = useLogs({
    dagId: taskInstance.dag_id,
    logLevelFilters: ["warning", "error", "critical"],
    taskInstance,
    tryNumber: taskInstance.try_number,
  });

  return (
    <Box borderRadius={4} borderStyle="solid" borderWidth={1} key={taskInstance.id} p={2} width="100%">
      <Flex justifyContent="space-between">
        <Box>
          <StateBadge mr={1} state={taskInstance.state} />
          {taskInstance.task_display_name}
          <Time datetime={taskInstance.run_after} ml={1} />
        </Box>
        <Flex gap={1}>
          <ClearTaskInstanceButton taskInstance={taskInstance} withText={false} />
          <Link asChild color="fg.info" fontSize="sm">
            <RouterLink to={getTaskInstanceLink(taskInstance)}>View full logs</RouterLink>
          </Link>
        </Flex>
      </Flex>
      <Box maxHeight="200px" overflow="auto">
        <TaskLogContent
          error={error}
          isLoading={isLoading}
          logError={error}
          parsedLogs={data.parsedLogs}
          wrap={defaultWrap}
        />
      </Box>
    </Box>
  );
};
