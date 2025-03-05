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
import { Badge } from "@chakra-ui/react";
import type { UseQueryOptions } from "@tanstack/react-query";
import dayjs from "dayjs";

import { useTaskInstanceServiceGetLog } from "openapi/queries";
import type {
  StructuredLogMessage,
  TaskInstanceResponse,
  TaskInstancesLogResponse,
} from "openapi/requests/types.gen";
import Time from "src/components/Time";
import { isStatePending, useAutoRefresh } from "src/utils";
import { LogLevel, logLevelColorMapping } from "src/utils/logs";

type Props = {
  dagId: string;
  logLevelFilters?: Array<string>;
  taskInstance?: TaskInstanceResponse;
  tryNumber?: number;
};

type ParseLogsProps = {
  data: TaskInstancesLogResponse["content"];
  logLevelFilters?: Array<string>;
};

const renderStructuredLog = (
  logMessage: string | StructuredLogMessage,
  index: number,
  logLevelFilters?: Array<string>,
) => {
  if (typeof logMessage === "string") {
    return <p key={index}>{logMessage}</p>;
  }

  const { event, level = undefined, timestamp, ...structured } = logMessage;

  const elements = [];

  if (
    logLevelFilters !== undefined &&
    Boolean(logLevelFilters.length) &&
    ((typeof level === "string" && !logLevelFilters.includes(level)) || !Boolean(level))
  ) {
    return "";
  }

  if (Boolean(timestamp)) {
    elements.push("[", <Time datetime={timestamp} key={0} />, "] ");
  }

  if (typeof level === "string") {
    elements.push(
      <Badge
        colorPalette={level.toUpperCase() in LogLevel ? logLevelColorMapping[level as LogLevel] : undefined}
        key={1}
        minH={3}
        size="sm"
      >
        {level.toUpperCase()}
      </Badge>,
      " - ",
    );
  }

  elements.push(
    <span className="event" key={2}>
      {event}
    </span>,
  );

  for (const key in structured) {
    if (Object.hasOwn(structured, key)) {
      elements.push(
        " ",
        <span className={`log-key ${key}`} key={`prop_${key}`}>
          {key}={JSON.stringify(structured[key])}
        </span>,
      );
    }
  }

  return <p key={index}>{elements}</p>;
};

// TODO: add support for log groups, colors, formats, filters
const parseLogs = ({ data, logLevelFilters }: ParseLogsProps) => {
  let warning;
  let parsedLines;

  try {
    parsedLines = data.map((datum, index) => renderStructuredLog(datum, index, logLevelFilters));
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : "An error occurred.";

    // eslint-disable-next-line no-console
    console.warn(`Error parsing logs: ${errorMessage}`);
    warning = "Unable to show logs. There was an error parsing logs.";

    return { data, warning };
  }

  return {
    fileSources: [],
    parsedLogs: parsedLines,
    warning,
  };
};

export const useLogs = (
  { dagId, logLevelFilters, taskInstance, tryNumber = 1 }: Props,
  options?: Omit<UseQueryOptions<TaskInstancesLogResponse>, "queryFn" | "queryKey">,
) => {
  const refetchInterval = useAutoRefresh({ dagId });

  const { data, ...rest } = useTaskInstanceServiceGetLog(
    {
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
    data: data?.content ?? [],
    logLevelFilters,
  });

  return { data: parsedData, ...rest };
};
