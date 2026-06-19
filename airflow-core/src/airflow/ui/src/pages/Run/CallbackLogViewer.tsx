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
import { Box, Button, Heading, HStack, Text } from "@chakra-ui/react";
import type { TFunction } from "i18next";
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { FiFileText } from "react-icons/fi";
import innerText from "react-innertext";

import { useDeadlinesServiceGetCallbackLogs } from "openapi/queries";
import type { TaskInstanceState, TaskInstancesLogResponse } from "openapi/requests/types.gen";
import { StateBadge } from "src/components/StateBadge";
import { renderStructuredLog } from "src/components/renderStructuredLog";
import { Dialog } from "src/components/ui";
import { TaskLogContent } from "src/pages/TaskInstance/Logs/TaskLogContent";
import type { ParsedLogEntry } from "src/queries/useLogs";
import { parseStreamingLogContent } from "src/utils/logs";

type CallbackLogViewerProps = {
  readonly callbackId: string;
  readonly callbackState?: TaskInstanceState | null;
  readonly dagId: string;
  readonly dagRunId: string;
};

/**
 * Parse callback log data using the same structured log rendering pipeline
 * as the task instance logs, providing consistent formatting, grouping, and display.
 */
const parseCallbackLogs = (
  data: TaskInstancesLogResponse["content"],
  translate: TFunction,
): Array<ParsedLogEntry> => {
  let lineNumber = 0;
  const lineNumbers = data.map((datum) => {
    const text = typeof datum === "string" ? datum : datum.event;

    if (text.includes("::group::") || text.includes("::endgroup::")) {
      return undefined;
    }
    const current = lineNumber;

    lineNumber += 1;

    return current;
  });

  const parsedLines = data
    .map((datum, index) =>
      renderStructuredLog({
        index: lineNumbers[index] ?? index,
        logLink: "",
        logMessage: datum,
        renderingMode: "jsx",
        showSource: false,
        showTimestamp: true,
        translate,
      }),
    )
    .filter((parsedLine) => parsedLine !== "");

  // Process group markers (::group:: / ::endgroup::) into structured entries
  type Group = { id: number; level: number; name: string };
  const groupStack: Array<Group> = [];
  const result: Array<ParsedLogEntry> = [];
  let nextGroupId = 0;

  for (const line of parsedLines) {
    const text = innerText(line);

    if (text.includes("::group::")) {
      const groupName = text.split("::group::")[1] as string;
      const id = nextGroupId;

      nextGroupId += 1;
      const level = groupStack.length;
      const parentGroup = groupStack[groupStack.length - 1];

      groupStack.push({ id, level, name: groupName });
      result.push({
        element: groupName,
        group: { id, level, parentId: parentGroup?.id, type: "header" },
      });
    } else if (text.includes("::endgroup::")) {
      groupStack.pop();
    } else {
      const currentGroup = groupStack[groupStack.length - 1];

      if (groupStack.length > 0 && currentGroup) {
        result.push({
          element: line,
          group: { id: currentGroup.id, level: currentGroup.level, type: "line" },
        });
      } else {
        result.push({ element: line });
      }
    }
  }

  return result;
};

export const CallbackLogViewer = ({ callbackId, callbackState, dagId, dagRunId }: CallbackLogViewerProps) => {
  const { t: translate } = useTranslation(["dag", "common"]);
  const [isOpen, setIsOpen] = useState(false);

  const { data, error, isLoading } = useDeadlinesServiceGetCallbackLogs(
    {
      callbackId,
      dagId,
      dagRunId,
    },
    undefined,
    { enabled: isOpen },
  );

  const parsedLogs = useMemo(() => {
    const content = parseStreamingLogContent(data);

    if (content.length === 0) {
      return [];
    }

    return parseCallbackLogs(content, translate);
  }, [data, translate]);

  return (
    <>
      <Button onClick={() => setIsOpen(true)} size="xs" variant="outline">
        <FiFileText />
        {translate("dag:callbackLogs.viewLogs")}
      </Button>
      <Dialog.Root onOpenChange={() => setIsOpen(false)} open={isOpen} scrollBehavior="inside" size="xl">
        <Dialog.Content backdrop p={4}>
          <Dialog.Header>
            <HStack gap={2}>
              <Heading size="sm">{translate("dag:callbackLogs.title")}</Heading>
              {callbackState !== undefined && callbackState !== null ? (
                <StateBadge size="sm" state={callbackState}>
                  {callbackState}
                </StateBadge>
              ) : undefined}
            </HStack>
          </Dialog.Header>
          <Dialog.CloseTrigger />
          <Dialog.Body display="flex" flexDirection="column" minHeight="300px" pb={2}>
            {!isLoading && parsedLogs.length === 0 && error === undefined ? (
              <Text color="fg.muted" fontSize="sm">
                {translate("dag:callbackLogs.noLogs")}
              </Text>
            ) : (
              <Box display="flex" flexDirection="column" flexGrow={1} minHeight={0}>
                <TaskLogContent
                  error={error}
                  expanded
                  isLoading={isLoading}
                  logError={error}
                  parsedLogs={parsedLogs}
                  wrap
                />
              </Box>
            )}
          </Dialog.Body>
        </Dialog.Content>
      </Dialog.Root>
    </>
  );
};
