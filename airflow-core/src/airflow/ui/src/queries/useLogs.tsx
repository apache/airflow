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
import type { UseQueryOptions } from "@tanstack/react-query";
import dayjs from "dayjs";
import type { TFunction } from "i18next";
import type { JSX } from "react";
import { useTranslation } from "react-i18next";
import innerText from "react-innertext";

import { useTaskInstanceServiceGetLog } from "openapi/queries";
import type { TaskInstanceResponse, TaskInstancesLogResponse } from "openapi/requests/types.gen";
import {
  extractTIContext,
  renderStructuredLog,
  renderTIContextPreamble,
} from "src/components/renderStructuredLog";
import { isStatePending, useAutoRefresh } from "src/utils";
import { getTaskInstanceLink } from "src/utils/links";
import { parseStreamingLogContent } from "src/utils/logs";

export type ParsedLogEntry = {
  element: JSX.Element | string | undefined;
  group?: { id: number; level: number; parentId?: number; type: "header" | "line" };
};

type Props = {
  accept?: "*/*" | "application/json" | "application/x-ndjson";
  dagId: string;
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

  const logLink = taskInstance ? `${getTaskInstanceLink(taskInstance)}?try_number=${tryNumber}` : "";

  try {
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

    parsedLines = data
      .map((datum, index) => {
        if (typeof datum !== "string" && "logger" in datum) {
          const source = datum.logger as string;

          if (!sources.includes(source)) {
            sources.push(source);
          }
        }

        return renderStructuredLog({
          index: lineNumbers[index] ?? index,
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

  const flatEntries: Array<ParsedLogEntry> = (() => {
    type Group = { id: number; level: number; name: string };
    const groupStack: Array<Group> = [];
    const result: Array<ParsedLogEntry> = [];
    let nextGroupId = 0;

    parsedLines.forEach((line) => {
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

        return;
      }

      if (text.includes("::endgroup::")) {
        groupStack.pop();

        return;
      }

      const currentGroup = groupStack[groupStack.length - 1];

      if (groupStack.length > 0 && currentGroup) {
        result.push({
          element: line,
          group: { id: currentGroup.id, level: currentGroup.level, type: "line" },
        });
      } else {
        result.push({ element: line });
      }
    });

    // Handle unclosed groups: their lines are already in result as flat entries
    return result;
  })();

  // Extract TI identity fields from the first structured log line and insert a single preamble
  // entry after the "Log message source details" group (or at position 0 if absent), so they
  // appear once rather than repeated on every line.
  const tiContext = extractTIContext(data);

  if (tiContext !== undefined) {
    let insertAt = 0;
    const sourceDetailsIndex = flatEntries.findIndex(
      (entry) =>
        entry.group?.type === "header" &&
        typeof entry.element === "string" &&
        entry.element.startsWith("Log message source details"),
    );

    const sourceGroup = sourceDetailsIndex === -1 ? undefined : flatEntries[sourceDetailsIndex];

    if (sourceGroup?.group !== undefined) {
      const sourceGroupId = sourceGroup.group.id;
      const lastMemberIndex = flatEntries.reduce(
        (last, entry, idx) => (entry.group?.id === sourceGroupId ? idx : last),
        sourceDetailsIndex,
      );

      insertAt = lastMemberIndex + 1;
    }
    flatEntries.splice(insertAt, 0, { element: renderTIContextPreamble(tiContext, "jsx", "Task Identity") });
  }

  return {
    parsedLogs: flatEntries,
    sources,
    warning,
  };
};

// Log truncation is performed in the frontend because the backend
// does not support yet pagination / limits on logs reading endpoint
const truncateData = (data: TaskInstancesLogResponse | undefined, limit?: number) => {
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
};

export const useLogs = (
  {
    accept = "application/x-ndjson",
    dagId,
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

  const parsedData = parseLogs({
    data: parseStreamingLogContent(truncateData(data, limit)),
    logLevelFilters,
    showSource,
    showTimestamp,
    sourceFilters,
    taskInstance,
    translate,
    tryNumber,
  });

  // Build a 1:1 searchable text array from parsedLogs so search indices align
  // with the rendered output. Each entry maps to exactly one line.
  const searchableText: Array<string> = (parsedData.parsedLogs ?? []).map((entry) => {
    if (typeof entry.element === "string") {
      return entry.element;
    }

    return entry.element ? innerText(entry.element) : "";
  });

  return { parsedData: { ...parsedData, searchableText }, ...rest, fetchedData: data };
};
