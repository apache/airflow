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
import { Text, Flex, Table, Tbody, Tr, Td, Code, Box } from "@chakra-ui/react";
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

interface Props {
  gridInstance?: GridTaskInstance;
  taskInstance?: API.TaskInstance;
  group?: Task | null;
}

const Details = ({ gridInstance, taskInstance, group }: Props) => {
  const isGroup = !!group?.children;
  const summary: React.ReactNode[] = [];

  const state =
    gridInstance?.state ||
    (taskInstance?.state === "none" ? null : taskInstance?.state) ||
    null;
  const isMapped = group?.isMapped;
  const runId = gridInstance?.runId || taskInstance?.dagRunId;
  const startDate = gridInstance?.startDate || taskInstance?.startDate;
  const endDate = gridInstance?.endDate || taskInstance?.endDate;
  const taskId = gridInstance?.taskId || taskInstance?.taskId;
  const mapIndex = gridInstance?.mapIndex || taskInstance?.mapIndex;

  const operator = group?.operator || taskInstance?.operator;

  const mappedStates = !taskInstance ? gridInstance?.mappedStates : undefined;

  let totalTasks;
  let childTaskMap;

  if (group) {
    const groupAndMapSummary = getGroupAndMapSummary({
      group,
      runId,
      mappedStates,
    });

    totalTasks = groupAndMapSummary.totalTasks;
    childTaskMap = groupAndMapSummary.childTaskMap;

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
  }

  const taskIdTitle = isGroup ? "Task Group ID" : "Task ID";
  const isStateFinal =
    state &&
    ["success", "failed", "upstream_failed", "skipped"].includes(state);
  const isOverall = (isMapped || isGroup) && "Overall ";

  return (
    <Box mt={3} flexGrow={1}>
      <Text as="strong" mb={3}>
        Task Instance Details
      </Text>
      <Table variant="striped">
        <Tbody>
          {group?.tooltip && (
            <Tr>
              <Td colSpan={2}>{group.tooltip}</Td>
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
          {!!group?.setupTeardownType && (
            <Tr>
              <Td>Type</Td>
              <Td>
                <Text textTransform="capitalize">
                  {group.setupTeardownType}
                </Text>
              </Td>
            </Tr>
          )}
          {mappedStates && !!totalTasks && totalTasks > 0 && (
            <Tr>
              <Td colSpan={2}>
                {totalTasks} {isGroup ? "Task Group" : "Task"}
                {totalTasks === 1 ? " " : "s "}
                Mapped
              </Td>
            </Tr>
          )}
          {summary.length > 0 && summary}
          {!!taskId && (
            <Tr>
              <Td>{taskIdTitle}</Td>
              <Td>
                <ClipboardText value={taskId} />
              </Td>
            </Tr>
          )}
          {!!runId && (
            <Tr>
              <Td>Run ID</Td>
              <Td>
                <Text whiteSpace="nowrap">
                  <ClipboardText value={runId} />
                </Text>
              </Td>
            </Tr>
          )}
          {mapIndex !== undefined && (
            <Tr>
              <Td>Map Index</Td>
              <Td>{mapIndex}</Td>
            </Tr>
          )}
          {taskInstance?.renderedMapIndex !== undefined &&
            taskInstance?.renderedMapIndex !== null && (
              <Tr>
                <Td>Rendered Map Index</Td>
                <Td>{taskInstance.renderedMapIndex}</Td>
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
          {group?.triggerRule && (
            <Tr>
              <Td>Trigger Rule</Td>
              <Td>{group.triggerRule}</Td>
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
      {taskInstance?.renderedFields && (
        <Box mt={3}>
          <Text as="strong" mb={3}>
            Rendered Templates
          </Text>
          <Table>
            <Tbody>
              {Object.keys(taskInstance.renderedFields).map((key) => {
                const renderedFields = taskInstance.renderedFields as Record<
                  string,
                  unknown
                >;
                let field = renderedFields[key];
                if (field) {
                  if (typeof field !== "string") {
                    try {
                      field = JSON.stringify(field);
                    } catch (e) {
                      // skip
                    }
                  }
                  return (
                    <Tr key={key}>
                      <Td>{key}</Td>
                      <Td>
                        <Code fontSize="md">{field as string}</Code>
                      </Td>
                    </Tr>
                  );
                }
                return null;
              })}
            </Tbody>
          </Table>
        </Box>
      )}
    </Box>
  );
};

export default Details;
