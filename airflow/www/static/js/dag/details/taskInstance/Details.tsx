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
import {
  Text,
  Flex,
  Table,
  Tbody,
  Tr,
  Td,
  AccordionItem,
  AccordionPanel,
} from "@chakra-ui/react";
import { snakeCase } from "lodash";

import { getGroupAndMapSummary } from "src/utils";
import { getDuration, formatDuration } from "src/datetime_utils";
import { SimpleStatus } from "src/dag/StatusBox";
import Time from "src/components/Time";
import { ClipboardText } from "src/components/Clipboard";
import type { Task, TaskInstance, TaskState } from "src/types";
import AccordionHeader from "src/components/AccordionHeader";

interface Props {
  instance: TaskInstance;
  group: Task;
}

const Details = ({ instance, group }: Props) => {
  const isGroup = !!group.children;
  const summary: React.ReactNode[] = [];

  const { taskId, runId, startDate, endDate, state, mappedStates, mapIndex } =
    instance;

  const { isMapped, tooltip, operator, triggerRule } = group;

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
    <AccordionItem>
      <AccordionHeader>Task Instance Details</AccordionHeader>
      <AccordionPanel>
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
      </AccordionPanel>
    </AccordionItem>
  );
};

export default Details;
