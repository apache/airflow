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
import { useTIHistory } from "src/api";
import { SimpleStatus } from "src/dag/StatusBox";
import { formatDuration, getDuration } from "src/datetime_utils";
import type { TaskInstance } from "src/types/api-generated";

interface Props {
  taskInstance: TaskInstance;
  selectedTryNumber?: number;
  onSelectTryNumber?: (tryNumber: number) => void;
}

const TrySelector = ({
  taskInstance,
  selectedTryNumber,
  onSelectTryNumber,
}: Props) => {
  const {
    taskId,
    dagRunId,
    dagId,
    mapIndex,
    tryNumber: finalTryNumber,
  } = taskInstance;
  const containerRef = useContainerRef();

  const { data: tiHistory } = useTIHistory({
    dagId: dagId || "",
    taskId: taskId || "",
    dagRunId: dagRunId || "",
    mapIndex,
    options: {
      enabled: !!(finalTryNumber && finalTryNumber > 1) && !!taskId, // Only try to look up task tries if try number > 1
    },
  });

  if (!finalTryNumber || finalTryNumber <= 1) return null;

  const logAttemptDropdownLimit = 10;
  const showDropdown = finalTryNumber > logAttemptDropdownLimit;

  const tries = (tiHistory?.taskInstances || []).filter(
    (t) => t?.startDate !== taskInstance?.startDate
  );
  tries?.push(taskInstance);

  return (
    <Box my={3}>
      <Text as="strong">Task Tries</Text>
      {!showDropdown && (
        <Flex my={1} flexWrap="wrap">
          {/* Even without try history showing up we should still show all try numbers */}
          {Array.from({ length: finalTryNumber }, (_, i) => i + 1).map(
            (tryNumber, i) => {
              let attempt;
              if (tries.length) {
                attempt = tries[i];
              }
              return (
                <Tooltip
                  key={tryNumber}
                  label={
                    !!attempt && (
                      <Box>
                        <Text>Status: {attempt.state}</Text>
                        <Text>
                          Duration:{" "}
                          {formatDuration(
                            getDuration(attempt.startDate, attempt.endDate)
                          )}
                        </Text>
                      </Box>
                    )
                  }
                  hasArrow
                  portalProps={{ containerRef }}
                  placement="top"
                  isDisabled={!attempt}
                >
                  <Button
                    key={tryNumber}
                    variant={
                      selectedTryNumber === tryNumber ? "solid" : "ghost"
                    }
                    colorScheme="blue"
                    onClick={() => onSelectTryNumber?.(tryNumber)}
                    data-testid={`log-attempt-select-button-${tryNumber}`}
                  >
                    {tryNumber}
                    {!!attempt && <SimpleStatus ml={2} state={attempt.state} />}
                  </Button>
                </Tooltip>
              );
            }
          )}
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
