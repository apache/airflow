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
import { Text, Flex, Table, Tbody, Tr, Td, Code } from "@chakra-ui/react";
import { snakeCase } from "lodash";

import { getGroupAndMapSummary } from "src/utils";
import { getDuration, formatDuration } from "src/datetime_utils";
import { SimpleStatus } from "src/dag/StatusBox";
import Time from "src/components/Time";
import { ClipboardText } from "src/components/Clipboard";
import type {
  API,
  Task,
  TaskInstance as GridTaskInstance,
  TaskState,
} from "src/types";
import DatasetUpdateEvents from "./DatasetUpdateEvents";

interface Props {
  gridInstance: GridTaskInstance;
  taskInstance?: API.TaskInstance;
  group: Task;
}

const Details = ({ gridInstance, taskInstance, group }: Props) => {
  const isGroup = !!group.children;
  const summary: React.ReactNode[] = [];

  const { taskId, runId, startDate, endDate, state } = gridInstance;

  const mappedStates = !taskInstance ? gridInstance.mappedStates : undefined;

  const { isMapped, tooltip, operator, hasOutletDatasets, triggerRule } = group;

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
      {!!taskInstance?.trigger && !!taskInstance?.triggererJob && (
        <>
          <Text as="strong" mb={3}>
            Triggerer info
          </Text>
          <Table variant="striped" mb={3}>
            <Tbody>
              <Tr>
                <Td>Trigger class</Td>
                <Td>{`${taskInstance?.trigger?.classpath}`}</Td>
              </Tr>
              <Tr>
                <Td>Trigger ID</Td>
                <Td>{`${taskInstance?.trigger?.id}`}</Td>
              </Tr>
              <Tr>
                <Td>Trigger creation time</Td>
                <Td>{`${taskInstance?.trigger?.createdDate}`}</Td>
              </Tr>
              <Tr>
                <Td>Assigned triggerer</Td>
                <Td>{`${taskInstance?.triggererJob?.hostname}`}</Td>
              </Tr>
              <Tr>
                <Td>Latest triggerer heartbeat</Td>
                <Td>{`${taskInstance?.triggererJob?.latestHeartbeat}`}</Td>
              </Tr>
            </Tbody>
          </Table>
        </>
      )}

      <Text as="strong" mb={3}>
        Task Instance Details
      </Text>
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
          {!!group.setupTeardownType && (
            <Tr>
              <Td>Type</Td>
              <Td>
                <Text textTransform="capitalize">
                  {group.setupTeardownType}
                </Text>
              </Td>
            </Tr>
          )}
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
          {taskInstance?.mapIndex !== undefined && (
            <Tr>
              <Td>Map Index</Td>
              <Td>{taskInstance.mapIndex}</Td>
            </Tr>
          )}
          {!!taskInstance?.tryNumber && (
            <Tr>
              <Td>Try Number</Td>
              <Td>{taskInstance.tryNumber}</Td>
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
          {!!taskInstance?.pid && (
            <Tr>
              <Td>Process ID (PID)</Td>
              <Td>
                <ClipboardText value={taskInstance.pid.toString()} />
              </Td>
            </Tr>
          )}
          {!!taskInstance?.hostname && (
            <Tr>
              <Td>Hostname</Td>
              <Td>
                <ClipboardText value={taskInstance.hostname} />
              </Td>
            </Tr>
          )}
          {taskInstance?.renderedFields && (
            <>
              {Object.keys(taskInstance.renderedFields).map((key) => {
                const renderedFields = taskInstance.renderedFields as Record<
                  string,
                  unknown
                >;
                if (renderedFields[key]) {
                  return (
                    <Tr key={key}>
                      <Td>{key}</Td>
                      <Td>
                        <Code fontSize="md">
                          {JSON.stringify(renderedFields[key])}
                        </Code>
                      </Td>
                    </Tr>
                  );
                }
                return null;
              })}
            </>
          )}
          {!!taskInstance?.pool && (
            <Tr>
              <Td>Pool</Td>
              <Td>{taskInstance.pool}</Td>
            </Tr>
          )}
          {!!taskInstance?.poolSlots && (
            <Tr>
              <Td>Pool Slots</Td>
              <Td>{taskInstance.poolSlots}</Td>
            </Tr>
          )}
          {!!taskInstance?.executorConfig && (
            <Tr>
              <Td>Executor Config</Td>
              <Td>
                <Code fontSize="md">{taskInstance.executorConfig}</Code>
              </Td>
            </Tr>
          )}
          {!!taskInstance?.unixname && (
            <Tr>
              <Td>Unix Name</Td>
              <Td>{taskInstance.unixname}</Td>
            </Tr>
          )}
          {!!taskInstance?.maxTries && (
            <Tr>
              <Td>Max Tries</Td>
              <Td>{taskInstance.maxTries}</Td>
            </Tr>
          )}
          {!!taskInstance?.queue && (
            <Tr>
              <Td>Queue</Td>
              <Td>{taskInstance.queue}</Td>
            </Tr>
          )}
          {!!taskInstance?.priorityWeight && (
            <Tr>
              <Td>Priority Weight</Td>
              <Td>{taskInstance.priorityWeight}</Td>
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
