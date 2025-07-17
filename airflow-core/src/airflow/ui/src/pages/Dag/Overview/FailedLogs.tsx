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
import { useTranslation } from "react-i18next";

import type { TaskInstanceCollectionResponse } from "openapi/requests/types.gen";
import { Tooltip } from "src/components/ui";
import { useConfig } from "src/queries/useConfig";

import { TaskLogPreview } from "./TaskLogPreview";

const FailedLogs = ({
  failedTasks,
}: {
  readonly failedTasks: TaskInstanceCollectionResponse | undefined;
}) => {
  const { t: translate } = useTranslation(["dag", "common"]);
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
        <Heading size="md">{translate("overview.failedLogs.title")}</Heading>
        <Tooltip closeDelay={100} content={translate("common:wrap.tooltip", { hotkey: "w" })} openDelay={100}>
          <Button
            aria-label={translate(`common:wrap.${wrap ? "un" : ""}wrap`)}
            bg="bg.panel"
            fontSize="sm"
            onClick={toggleWrap}
            size="sm"
            variant="outline"
          >
            {translate(`common:wrap.${wrap ? "un" : ""}wrap`)}
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
