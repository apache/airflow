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

import React, { useEffect, useState } from "react";
import { Text, Flex, Table, Tbody, Tr, Td, Code, Box } from "@chakra-ui/react";
import { snakeCase } from "lodash";

import { useTIHistory } from "src/api";
import { getGroupAndMapSummary, getMetaValue } from "src/utils";
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
import RenderedJsonField from "src/components/RenderedJsonField";
import TrySelector from "./TrySelector";

interface Props {
  gridInstance?: GridTaskInstance;
  taskInstance?: API.TaskInstance;
  group?: Task | null;
}

const dagId = getMetaValue("dag_id");

const Details = ({ gridInstance, taskInstance, group }: Props) => {
  const isGroup = !!group?.children;
  const summary: React.ReactNode[] = [];

  const { runId, taskId } = gridInstance || {};

  const finalTryNumber = gridInstance?.tryNumber || taskInstance?.tryNumber;

  const { data: tiHistory } = useTIHistory({
    dagId,
    taskId: taskId || "",
    dagRunId: runId || "",
    mapIndex: taskInstance?.mapIndex || -1,
    options: {
      enabled: !!(finalTryNumber && finalTryNumber > 1) && !!taskId, // Only try to look up task tries if try number > 1
    },
  });

  const [selectedTryNumber, setSelectedTryNumber] = useState(
    finalTryNumber || 1
  );

  // update state if the final try number changes
  useEffect(() => {
    if (finalTryNumber) setSelectedTryNumber(finalTryNumber);
  }, [finalTryNumber]);

  const tryInstance = tiHistory?.taskInstances?.find(
    (ti) => ti.tryNumber === selectedTryNumber
  );

  const instance =
    selectedTryNumber !== finalTryNumber && finalTryNumber && finalTryNumber > 1
      ? tryInstance
      : taskInstance;

  const state =
    instance?.state ||
    (instance?.state === "none" ? null : instance?.state) ||
    gridInstance?.state ||
    null;
  const isMapped = group?.isMapped;
  const startDate = instance?.startDate;
  const endDate = instance?.endDate;
  const executor = instance?.executor || "<default>";

  const operator = instance?.operator || group?.operator;

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
      {!!taskInstance && (
        <TrySelector
          taskInstance={taskInstance}
          selectedTryNumber={selectedTryNumber || finalTryNumber}
          onSelectTryNumber={setSelectedTryNumber}
        />
      )}
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
          {instance?.mapIndex !== undefined && (
            <Tr>
              <Td>Map Index</Td>
              <Td>{instance.mapIndex}</Td>
            </Tr>
          )}
          {instance?.renderedMapIndex !== undefined &&
            instance?.renderedMapIndex !== null && (
              <Tr>
                <Td>Rendered Map Index</Td>
                <Td>{instance.renderedMapIndex}</Td>
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
          {!!instance?.pid && (
            <Tr>
              <Td>Process ID (PID)</Td>
              <Td>
                <ClipboardText value={instance.pid.toString()} />
              </Td>
            </Tr>
          )}
          {!!instance?.hostname && (
            <Tr>
              <Td>Hostname</Td>
              <Td>
                <ClipboardText value={instance.hostname} />
              </Td>
            </Tr>
          )}
          {!!instance?.pool && (
            <Tr>
              <Td>Pool</Td>
              <Td>{instance.pool}</Td>
            </Tr>
          )}
          {!!instance?.poolSlots && (
            <Tr>
              <Td>Pool Slots</Td>
              <Td>{instance.poolSlots}</Td>
            </Tr>
          )}
          {executor && (
            <Tr>
              <Td>Executor</Td>
              <Td>{executor}</Td>
            </Tr>
          )}
          {!!instance?.executorConfig && (
            <Tr>
              <Td>Executor Config</Td>
              <Td>
                <Code fontSize="md">{instance.executorConfig.toString()}</Code>
              </Td>
            </Tr>
          )}
          {!!instance?.unixname && (
            <Tr>
              <Td>Unix Name</Td>
              <Td>{instance.unixname}</Td>
            </Tr>
          )}
          {!!instance?.maxTries && (
            <Tr>
              <Td>Max Tries</Td>
              <Td>{instance.maxTries}</Td>
            </Tr>
          )}
          {!!instance?.queue && (
            <Tr>
              <Td>Queue</Td>
              <Td>{instance.queue}</Td>
            </Tr>
          )}
          {!!instance?.priorityWeight && (
            <Tr>
              <Td>Priority Weight</Td>
              <Td>{instance.priorityWeight}</Td>
            </Tr>
          )}
        </Tbody>
      </Table>
      {instance?.renderedFields && (
        <Box mt={3}>
          <Text as="strong" mb={3}>
            Rendered Templates
          </Text>
          <Table variant="striped">
            <Tbody>
              {Object.keys(instance.renderedFields).map((key) => {
                const renderedFields = instance.renderedFields as Record<
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
                        <RenderedJsonField content={field as string} />
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
