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
import { chakra, Code } from "@chakra-ui/react";
import type { UseQueryOptions } from "@tanstack/react-query";
import dayjs from "dayjs";
import innerText from "react-innertext";
import { Link } from "react-router-dom";

import { useTaskInstanceServiceGetLog } from "openapi/queries";
import type {
  StructuredLogMessage,
  TaskInstanceResponse,
  TaskInstancesLogResponse,
} from "openapi/requests/types.gen";
import Time from "src/components/Time";
import { isStatePending, useAutoRefresh } from "src/utils";
import { getTaskInstanceLink } from "src/utils/links";
import { LogLevel, logLevelColorMapping } from "src/utils/logs";

type Props = {
  dagId: string;
  logLevelFilters?: Array<string>;
  sourceFilters?: Array<string>;
  taskInstance?: TaskInstanceResponse;
  tryNumber?: number;
};

type ParseLogsProps = {
  data: TaskInstancesLogResponse["content"];
  logLevelFilters?: Array<string>;
  sourceFilters?: Array<string>;
  taskInstance?: TaskInstanceResponse;
};

type RenderStructuredLogProps = {
  index: number;
  logLevelFilters?: Array<string>;
  logLink: string;
  logMessage: string | StructuredLogMessage;
  sourceFilters?: Array<string>;
};

const renderStructuredLog = ({
  index,
  logLevelFilters,
  logLink,
  logMessage,
  sourceFilters,
}: RenderStructuredLogProps) => {
  if (typeof logMessage === "string") {
    return (
      <chakra.span key={index} lineHeight={1.5}>
        {logMessage}
      </chakra.span>
    );
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

  if (
    sourceFilters !== undefined &&
    Boolean(sourceFilters.length) &&
    (("logger" in structured && !sourceFilters.includes(structured.logger as string)) ||
      !("logger" in structured))
  ) {
    return "";
  }

  elements.push(
    <Link
      id={index.toString()}
      key={`line_${index}`}
      style={{
        display: "inline-block",
        marginRight: "10px",
        paddingRight: "5px",
        textAlign: "right",
        userSelect: "none",
        WebkitUserSelect: "none",
        width: "3em",
      }}
      to={`${logLink}#${index}`}
    >
      {index}
    </Link>,
  );

  if (Boolean(timestamp)) {
    elements.push("[", <Time datetime={timestamp} key={0} />, "] ");
  }

  if (typeof level === "string") {
    elements.push(
      <Code
        colorPalette={level.toUpperCase() in LogLevel ? logLevelColorMapping[level as LogLevel] : undefined}
        key={1}
        lineHeight={1.5}
        minH={0}
        px={0}
      >
        {level.toUpperCase()}
      </Code>,
      " - ",
    );
  }

  elements.push(
    <chakra.span className="event" key={2} style={{ whiteSpace: "pre-wrap" }}>
      {event}
    </chakra.span>,
  );

  for (const key in structured) {
    if (Object.hasOwn(structured, key)) {
      elements.push(
        " ",
        <chakra.span color={key === "logger" ? "fg.info" : undefined} key={`prop_${key}`}>
          {key === "logger" ? "source" : key}={JSON.stringify(structured[key])}
        </chakra.span>,
      );
    }
  }

  return (
    <chakra.p key={index} lineHeight={1.5}>
      {elements}
    </chakra.p>
  );
};

const parseLogs = ({ data, logLevelFilters, sourceFilters, taskInstance }: ParseLogsProps) => {
  let warning;
  let parsedLines;
  let startGroup = false;
  let groupLines: Array<JSX.Element | ""> = [];
  let groupName = "";
  const sources: Array<string> = [];

  // open the summary when hash is present since the link might have a hash linking to a line
  const open = Boolean(location.hash);
  const logLink = taskInstance ? getTaskInstanceLink(taskInstance) : "";

  try {
    parsedLines = data.map((datum, index) => {
      if (typeof datum !== "string" && "logger" in datum) {
        const source = datum.logger as string;

        if (!sources.includes(source)) {
          sources.push(source);
        }
      }

      return renderStructuredLog({ index, logLevelFilters, logLink, logMessage: datum, sourceFilters });
    });
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : "An error occurred.";

    // eslint-disable-next-line no-console
    console.warn(`Error parsing logs: ${errorMessage}`);
    warning = "Unable to show logs. There was an error parsing logs.";

    return { data, warning };
  }

  // TODO: Add support for nested groups

  parsedLines = parsedLines.map((line) => {
    const text = innerText(line);

    if (text.includes("::group::") && !startGroup) {
      startGroup = true;
      groupName = text.split("::group::")[1] as string;
    } else if (text.includes("::endgroup::")) {
      startGroup = false;
      const group = (
        <details key={groupName} open={open} style={{ width: "100%" }}>
          <summary data-testid={`summary-${groupName}`}>
            <chakra.span color="fg.info" cursor="pointer">
              {groupName}
            </chakra.span>
          </summary>
          {groupLines}
        </details>
      );

      groupLines = [];

      return group;
    }

    if (startGroup) {
      groupLines.push(line);

      return undefined;
    } else {
      return line;
    }
  });

  return {
    parsedLogs: parsedLines,
    sources,
    warning,
  };
};

export const useLogs = (
  { dagId, logLevelFilters, sourceFilters, taskInstance, tryNumber = 1 }: Props,
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
    sourceFilters,
    taskInstance,
  });

  return { data: parsedData, ...rest };
};
