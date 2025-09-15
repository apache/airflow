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
import { type MouseEvent, useMemo } from "react";
import { useTranslation } from "react-i18next";

import { useTaskInstanceServiceGetLog } from "openapi/queries";
import type {
  StructuredLogMessage,
  TaskInstanceResponse,
  TaskInstancesLogResponse,
} from "openapi/requests/types.gen";
import Time from "src/components/Time";
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
    type Group = { level: number; lines: Array<JSX.Element | "">; name: string; timestamp?: string };
    const groupStack: Array<Group> = [];
    const result: Array<JSX.Element | ""> = [];

    parsedLines.forEach((line, idx) => {
      const log = data[idx];

      if (log === undefined) {
        return;
      }

      const [structured, event]: [StructuredLogMessage | undefined, string] =
        typeof log === "string" ? [undefined, log] : [log, log.event];

      if (event.includes("::group::")) {
        const groupName = event.split("::group::")[1] as string;

        const group: Group = { level: groupStack.length, lines: [], name: groupName };

        if (structured !== undefined && Object.hasOwn(structured, "timestamp")) {
          group.timestamp = structured.timestamp;
        }
        groupStack.push(group);
      }

      if (event.includes("::endgroup::")) {
        const finishedGroup = groupStack.pop();
        const elements: Array<JSX.Element | string> = [];

        if (finishedGroup) {
          if (Boolean(finishedGroup.timestamp) && showTimestamp) {
            elements.push("[", <Time datetime={finishedGroup.timestamp} key={0} />, "] ");
          }
          elements.push(finishedGroup.name);

          // If there is only a few lines in the group, don't show the end group
          if (finishedGroup.lines.length > 3) {
            const handleClick = (evt: MouseEvent<HTMLElement>) => {
              // Close the details when the End of Group line is clicked
              const details = evt.currentTarget.closest("details");

              if (details) {
                details.open = false;
              }
            };

            const endTimestamp =
              Boolean(structured?.timestamp) && showTimestamp
                ? ["[", <Time datetime={structured?.timestamp} key={0} />, "] "]
                : undefined;

            finishedGroup.lines.push(
              <Box key={finishedGroup.name} ml={3} pl={finishedGroup.level * 2}>
                <chakra.span color="fg.info" cursor="pointer" onClick={handleClick}>
                  {endTimestamp}
                  {translate("components:logs.endGroup", { group: finishedGroup.name })}
                </chakra.span>
              </Box>,
            );
          }

          const groupElement = (
            <chakra.details
              css={{ "&[open]": { "margin-bottom": 0 } }}
              key={finishedGroup.name}
              mb={2}
              open={open}
              pl={finishedGroup.level * 2}
              w="100%"
            >
              <chakra.summary data-testid={`summary-${finishedGroup.name}`}>
                <chakra.span color="fg.info" cursor="pointer">
                  {elements}
                </chakra.span>
              </chakra.summary>
              {finishedGroup.lines}
            </chakra.details>
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

  return { data: parsedData, ...rest };
};
