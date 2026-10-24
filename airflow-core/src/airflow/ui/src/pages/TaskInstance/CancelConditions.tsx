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
import { Box, Heading, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { useTaskServiceGetTask } from "openapi/queries";
import type { TaskInstanceHistoryResponse, TaskInstanceResponse } from "openapi/requests/types.gen";

import { WebhookMatchList } from "src/components/WebhookMatchList";

type CancelConditionsProps = {
  readonly taskInstance: TaskInstanceHistoryResponse | TaskInstanceResponse | undefined;
};

/**
 * The `cancel_on` conditions of the task in the Dag's latest version: a delivery is matched against those,
 * whatever version the task instance ran.
 */
export const CancelConditions = ({ taskInstance }: CancelConditionsProps) => {
  const { t: translate } = useTranslation("common");

  const { data: task } = useTaskServiceGetTask(
    {
      dagId: taskInstance?.dag_id ?? "",
      taskId: taskInstance?.task_id ?? "",
    },
    undefined,
    { enabled: taskInstance !== undefined },
  );

  if (task?.cancel_on === null || task?.cancel_on === undefined || task.cancel_on.length === 0) {
    return undefined;
  }

  return (
    <Box data-testid="cancel-conditions" py={1}>
      <Heading py={1} size="sm">
        {translate("taskInstance.cancelOn.title")}
      </Heading>
      <Text color="fg.muted" pb={1}>
        {translate("taskInstance.cancelOn.explanation", {
          state: translate(`states.${task.cancel_state ?? "failed"}`),
        })}
      </Text>
      <WebhookMatchList matches={task.cancel_on} />
    </Box>
  );
};
