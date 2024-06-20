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
import { Text, Box, Flex, Button, Select } from "@chakra-ui/react";
import Tooltip from "src/components/Tooltip";
import { useContainerRef } from "src/context/containerRef";
import { useTIHistory, useTaskInstance } from "src/api";
import { getMetaValue } from "src/utils";
import { SimpleStatus } from "src/dag/StatusBox";
import { formatDuration, getDuration } from "src/datetime_utils";

const dagId = getMetaValue("dag_id");

interface Props {
  taskId?: string;
  runId?: string;
  mapIndex?: number;
  selectedTryNumber?: number;
  onSelectTryNumber?: (tryNumber: number) => void;
}

const TrySelector = ({
  taskId,
  runId,
  mapIndex,
  selectedTryNumber,
  onSelectTryNumber,
}: Props) => {
  const containerRef = useContainerRef();

  const { data: taskInstance } = useTaskInstance({
    dagId,
    dagRunId: runId || "",
    taskId: taskId || "",
    mapIndex,
  });
  const finalTryNumber = taskInstance?.tryNumber;
  const { data: tiHistory } = useTIHistory({
    dagId,
    taskId: taskId || "",
    runId: runId || "",
    mapIndex,
    enabled: !!(finalTryNumber && finalTryNumber > 1) && !!taskId, // Only try to look up task tries if try number > 1
  });

  if (!finalTryNumber || finalTryNumber <= 1) return null;

  const logAttemptDropdownLimit = 10;
  const showDropdown = finalTryNumber > logAttemptDropdownLimit;

  const tries = (tiHistory || []).filter(
    (t) => t?.startDate !== taskInstance?.startDate
  );
  tries?.push(taskInstance);

  return (
    <Box my={3}>
      <Text as="strong">Task Tries</Text>
      {!showDropdown && (
        <Flex my={1} flexWrap="wrap">
          {tries.map(({ tryNumber, state, startDate, endDate }, i) => (
            <Tooltip
              key={tryNumber}
              label={
                <Box>
                  <Text>Status: {state}</Text>
                  <Text>
                    Duration: {formatDuration(getDuration(startDate, endDate))}
                  </Text>
                </Box>
              }
              hasArrow
              portalProps={{ containerRef }}
              placement="top"
            >
              <Button
                variant={selectedTryNumber === tryNumber ? "solid" : "ghost"}
                colorScheme="blue"
                onClick={() => onSelectTryNumber?.(tryNumber || i)}
                data-testid={`log-attempt-select-button-${tryNumber}`}
              >
                <Flex>
                  <Text mr={2}>{tryNumber}</Text>
                  <SimpleStatus state={state} />
                </Flex>
              </Button>
            </Tooltip>
          ))}
        </Flex>
      )}
      {showDropdown && (
        <Select
          onChange={(e) => {
            onSelectTryNumber?.(Number(e.target.value));
          }}
          value={selectedTryNumber}
          maxWidth="200px"
        >
          {tries.map(({ tryNumber, state }) => (
            <option key={tryNumber} value={tryNumber}>
              {tryNumber}: {state}
            </option>
          ))}
        </Select>
      )}
    </Box>
  );
};

export default TrySelector;
