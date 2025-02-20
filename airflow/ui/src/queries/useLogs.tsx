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
import { Text } from "@chakra-ui/react";
import dayjs from "dayjs";

import { useTaskInstanceServiceGetLog } from "openapi/queries";
import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { isStatePending, useAutoRefresh } from "src/utils";

type Props = {
  dagId: string;
  taskInstance?: TaskInstanceResponse;
  tryNumber?: number;
};

type ParseLogsProps = {
  data: string | undefined;
};

// TODO: add support for log groups, colors, formats, filters
const parseLogs = ({ data }: ParseLogsProps) => {
  if (data === undefined) {
    return {};
  }
  let lines;

  let warning;

  try {
    lines = data.split("\\n");
  } catch {
    warning = "Unable to show logs. There was an error parsing logs.";

    return { data, warning };
  }

  let startGroup = false;
  let groupLines: Array<string> = [];
  let groupName = "";

  // TODO: Add support for nested groups
  /* eslint-disable react/no-array-index-key */
  const parsedLines = lines.map((line, index) => {
    if (line.includes("::group::") && !startGroup) {
      startGroup = true;
      groupName = line.split("::group::")[1] as string;
    } else if (line.includes("::endgroup::")) {
      startGroup = false;
      groupLines.push(line);
      const group = (
        <details key={groupName}>
          <summary data-testid={`summary-${groupName}`}>
            <Text as="span" color="fg.info" cursor="pointer">
              {groupName}
            </Text>
          </summary>
          {groupLines.map((text, groupIndex) => (
            <Text key={groupIndex} py={1}>
              {text}
            </Text>
          ))}
        </details>
      );

      groupLines = [];

      return group;
    }

    if (startGroup) {
      groupLines.push(line);

      return undefined;
    } else {
      return <Text key={index}>{line}</Text>;
    }
  });

  return {
    fileSources: [],
    parsedLogs: parsedLines,
    warning,
  };
};

export const useLogs = ({ dagId, taskInstance, tryNumber = 1 }: Props) => {
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
    },
  );

  const parsedData = parseLogs({
    data: data?.content,
  });

  return { data: parsedData, ...rest };
};
