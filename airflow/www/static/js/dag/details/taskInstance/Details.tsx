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

import React from "react";
import { Text, Flex, Table, Tbody, Tr, Td, Divider } from "@chakra-ui/react";
import { snakeCase } from "lodash";

import { getGroupAndMapSummary } from "src/utils";
import { getDuration, formatDuration } from "src/datetime_utils";
import { SimpleStatus } from "src/dag/StatusBox";
import Time from "src/components/Time";
import { ClipboardText } from "src/components/Clipboard";
import type { Task, TaskInstance, TaskState } from "src/types";
import useTaskInstance from "src/api/useTaskInstance";
import DatasetUpdateEvents from "./DatasetUpdateEvents";

interface Props {
  instance: TaskInstance;
  group: Task;
  dagId: string;
}

const Details = ({ instance, group, dagId }: Props) => {
  const isGroup = !!group.children;
  const summary: React.ReactNode[] = [];

  const { taskId, runId, startDate, endDate, state, mappedStates, mapIndex } =
    instance;

  const { isMapped, tooltip, operator, hasOutletDatasets, triggerRule } = group;

  const { data: apiTI } = useTaskInstance({
    dagId,
    dagRunId: runId,
    taskId,
    mapIndex,
    enabled: !isGroup && !isMapped,
  });

  const { totalTasks, childTaskMap } = getGroupAndMapSummary({
    group,
    runId,
    mappedStates,
  });

  childTaskMap.forEach((key, val) => {
    const childState = snakeCase(val);
    if (key > 0) {
      summary.push(
        <Tr key={childState}>
          <Td />
          <Td>
            <Flex alignItems="center">
              <SimpleStatus state={childState as TaskState} mx={2} />
              {childState}
              {": "}
              {key}
            </Flex>
          </Td>
        </Tr>
      );
    }
  });

  const taskIdTitle = isGroup ? "Task Group ID" : "Task ID";
  const isStateFinal =
    state &&
    ["success", "failed", "upstream_failed", "skipped"].includes(state);
  const isOverall = (isMapped || isGroup) && "Overall ";
  return (
    <Flex flexWrap="wrap" justifyContent="space-between">
      {state === "deferred" && (
        <>
          <Text as="strong">Triggerer info</Text>
          <Divider my={2} />
          <Table variant="striped" mb={3}>
            <Tbody>
              <Tr>
                <Td>Trigger class</Td>
                <Td>{`${apiTI?.trigger?.classpath}`}</Td>
              </Tr>
              <Tr>
                <Td>Trigger creation time</Td>
                <Td>{`${apiTI?.trigger?.createdDate}`}</Td>
              </Tr>
              <Tr>
                <Td>Assigned triggerer</Td>
                <Td>{`${apiTI?.triggererJob?.hostname}`}</Td>
              </Tr>
              <Tr>
                <Td>Latest triggerer heartbeat</Td>
                <Td>{`${apiTI?.triggererJob?.latestHeartbeat}`}</Td>
              </Tr>
            </Tbody>
          </Table>
        </>
      )}

      <Text as="strong">Task Instance Details</Text>
      <Divider my={2} />
      <Table variant="striped">
        <Tbody>
          {tooltip && (
            <Tr>
              <Td colSpan={2}>{tooltip}</Td>
            </Tr>
          )}
          <Tr>
            <Td>
              {isOverall}
              Status
            </Td>
            <Td>
              <Flex>
                <SimpleStatus state={state} mx={2} />
                {state || "no status"}
              </Flex>
            </Td>
          </Tr>
          {mappedStates && totalTasks > 0 && (
            <Tr>
              <Td colSpan={2}>
                {totalTasks} {isGroup ? "Task Group" : "Task"}
                {totalTasks === 1 ? " " : "s "}
                Mapped
              </Td>
            </Tr>
          )}
          {summary.length > 0 && summary}
          <Tr>
            <Td>{taskIdTitle}</Td>
            <Td>
              <ClipboardText value={taskId} />
            </Td>
          </Tr>
          <Tr>
            <Td>Run ID</Td>
            <Td>
              <Text whiteSpace="nowrap">
                <ClipboardText value={runId} />
              </Text>
            </Td>
          </Tr>
          {mapIndex !== undefined && (
            <Tr>
              <Td>Map Index</Td>
              <Td>{mapIndex}</Td>
            </Tr>
          )}
          {operator && (
            <Tr>
              <Td>Operator</Td>
              <Td>{operator}</Td>
            </Tr>
          )}
          {triggerRule && (
            <Tr>
              <Td>Trigger Rule</Td>
              <Td>{triggerRule}</Td>
            </Tr>
          )}
          {startDate && (
            <Tr>
              <Td>
                {isOverall}
                Duration
              </Td>
              <Td>{formatDuration(getDuration(startDate, endDate))}</Td>
            </Tr>
          )}
          {startDate && (
            <Tr>
              <Td>Started</Td>
              <Td>
                <Time dateTime={startDate} />
              </Td>
            </Tr>
          )}
          {endDate && isStateFinal && (
            <Tr>
              <Td>Ended</Td>
              <Td>
                <Time dateTime={endDate} />
              </Td>
            </Tr>
          )}
        </Tbody>
      </Table>
      {hasOutletDatasets && (
        <DatasetUpdateEvents taskId={taskId} runId={runId} />
      )}
    </Flex>
  );
};

export default Details;
