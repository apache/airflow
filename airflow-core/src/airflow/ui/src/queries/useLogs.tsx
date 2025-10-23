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
import { chakra, Box } from "@chakra-ui/react";
import type { UseQueryOptions } from "@tanstack/react-query";
import dayjs from "dayjs";
import type { TFunction } from "i18next";
import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import innerText from "react-innertext";

import { useTaskInstanceServiceGetLog } from "openapi/queries";
import type { TaskInstanceResponse, TaskInstancesLogResponse } from "openapi/requests/types.gen";
import { renderStructuredLog } from "src/components/renderStructuredLog";
import { isStatePending, useAutoRefresh } from "src/utils";
import { getTaskInstanceLink } from "src/utils/links";
import { parseStreamingLogContent } from "src/utils/logs";

type Props = {
  accept?: "*/*" | "application/json" | "application/x-ndjson";
  dagId: string;
  expanded?: boolean;
  limit?: number;
  logLevelFilters?: Array<string>;
  showSource?: boolean;
  showTimestamp?: boolean;
  sourceFilters?: Array<string>;
  taskInstance?: TaskInstanceResponse;
  tryNumber?: number;
};

type ParseLogsProps = {
  data: TaskInstancesLogResponse["content"];
  expanded?: boolean;
  logLevelFilters?: Array<string>;
  showSource?: boolean;
  showTimestamp?: boolean;
  sourceFilters?: Array<string>;
  taskInstance?: TaskInstanceResponse;
  translate: TFunction;
  tryNumber: number;
};

const parseLogs = ({
  data,
  expanded,
  logLevelFilters,
  showSource,
  showTimestamp,
  sourceFilters,
  taskInstance,
  translate,
  tryNumber,
}: ParseLogsProps) => {
  let warning;
  let parsedLines;
  const sources: Array<string> = [];

  const open = expanded ?? Boolean(globalThis.location.hash);
  const logLink = taskInstance ? `${getTaskInstanceLink(taskInstance)}?try_number=${tryNumber}` : "";

  try {
    parsedLines = data
      .map((datum, index) => {
        if (typeof datum !== "string" && "logger" in datum) {
          const source = datum.logger as string;

          if (!sources.includes(source)) {
            sources.push(source);
          }
        }

        return renderStructuredLog({
          index,
          logLevelFilters,
          logLink,
          logMessage: datum,
          renderingMode: "jsx",
          showSource,
          showTimestamp,
          sourceFilters,
          translate,
        });
      })
      .filter((parsedLine) => parsedLine !== "");
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : "An error occurred.";

    // eslint-disable-next-line no-console
    console.warn(`Error parsing logs: ${errorMessage}`);
    warning = "Unable to show logs. There was an error parsing logs.";

    return { data, warning };
  }

  parsedLines = (() => {
    type Group = { level: number; lines: Array<JSX.Element | "">; name: string };
    const groupStack: Array<Group> = [];
    const result: Array<JSX.Element | ""> = [];

    parsedLines.forEach((line) => {
      const text = innerText(line);

      if (text.includes("::group::")) {
        const groupName = text.split("::group::")[1] as string;

        groupStack.push({ level: groupStack.length, lines: [], name: groupName });

        return;
      }

      if (text.includes("::endgroup::")) {
        const finishedGroup = groupStack.pop();

        if (finishedGroup) {
          const groupElement = (
            <Box key={finishedGroup.name} mb={2} pl={finishedGroup.level * 2}>
              <chakra.details open={open} w="100%">
                <chakra.summary data-testid={`summary-${finishedGroup.name}`}>
                  <chakra.span color="fg.info" cursor="pointer">
                    {finishedGroup.name}
                  </chakra.span>
                </chakra.summary>
                {finishedGroup.lines}
              </chakra.details>
            </Box>
          );

          const lastGroup = groupStack[groupStack.length - 1];

          if (groupStack.length > 0 && lastGroup) {
            lastGroup.lines.push(groupElement);
          } else {
            result.push(groupElement);
          }
        }

        return;
      }

      if (groupStack.length > 0 && groupStack[groupStack.length - 1]) {
        groupStack[groupStack.length - 1]?.lines.push(line);
      } else {
        result.push(line);
      }
    });

    while (groupStack.length > 0) {
      const unfinished = groupStack.pop();

      if (unfinished) {
        result.push(
          <Box key={unfinished.name} mb={2} pl={unfinished.level * 2}>
            {unfinished.lines}
          </Box>,
        );
      }
    }

    return result;
  })();

  return {
    parsedLogs: parsedLines,
    sources,
    warning,
  };
};

export const useLogs = (
  {
    accept = "application/x-ndjson",
    dagId,
    expanded,
    limit,
    logLevelFilters,
    showSource,
    showTimestamp,
    sourceFilters,
    taskInstance,
    tryNumber = 1,
  }: Props,
  options?: Omit<UseQueryOptions<TaskInstancesLogResponse>, "queryFn" | "queryKey">,
) => {
  const { t: translate } = useTranslation("common");
  const refetchInterval = useAutoRefresh({ dagId });

  const { data, ...rest } = useTaskInstanceServiceGetLog(
    {
      accept,
      dagId,
      dagRunId: taskInstance?.dag_run_id ?? "",
      mapIndex: taskInstance?.map_index ?? -1,
      taskId: taskInstance?.task_id ?? "",
      tryNumber,
    },
    undefined,
    {
      enabled: Boolean(taskInstance),
      refetchInterval: (query) =>
        isStatePending(taskInstance?.state) ||
        dayjs(query.state.dataUpdatedAt).isBefore(taskInstance?.end_date)
          ? refetchInterval
          : false,
      ...options,
    },
  );

  // Log truncation is performed in the frontend because the backend
  // does not support yet pagination / limits on logs reading endpoint
  const truncatedData = useMemo(() => {
    if (!data?.content || limit === undefined || limit <= 0) {
      return data;
    }

    const streamingContent = parseStreamingLogContent(data);
    const truncatedContent =
      streamingContent.length > limit ? streamingContent.slice(-limit) : streamingContent;

    return {
      ...data,
      content: truncatedContent,
    };
  }, [data, limit]);

  const parsedData = parseLogs({
    data: parseStreamingLogContent(truncatedData),
    expanded,
    logLevelFilters,
    showSource,
    showTimestamp,
    sourceFilters,
    taskInstance,
    translate,
    tryNumber,
  });

  return { parsedData, ...rest, fetchedData: data };
};
