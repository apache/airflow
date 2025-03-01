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
import { Badge, Box, HStack, IconButton, type SelectValueChangeDetails } from "@chakra-ui/react";
import { useCallback } from "react";
import { MdOutlineOpenInFull } from "react-icons/md";
import { useSearchParams } from "react-router-dom";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { TaskTrySelect } from "src/components/TaskTrySelect";
import { Button, Select } from "src/components/ui";
import { SearchParamsKeys } from "src/constants/searchParams";
import { type LogLevel, logLevelColorMapping, logLevelOptions } from "src/utils/logs";

type Props = {
  readonly isFullscreen?: boolean;
  readonly onSelectTryNumber: (tryNumber: number) => void;
  readonly taskInstance?: TaskInstanceResponse;
  readonly toggleFullscreen: () => void;
  readonly toggleWrap: () => void;
  readonly tryNumber?: number;
  readonly wrap: boolean;
};

export const TaskLogHeader = ({
  isFullscreen = false,
  onSelectTryNumber,
  taskInstance,
  toggleFullscreen,
  toggleWrap,
  tryNumber,
  wrap,
}: Props) => {
  const [searchParams, setSearchParams] = useSearchParams();

  const logLevels = searchParams.getAll(SearchParamsKeys.LOG_LEVEL);
  const hasLogLevels = logLevels.length > 0;

  const handleStateChange = useCallback(
    ({ value }: SelectValueChangeDetails<string>) => {
      const [val, ...rest] = value;

      if ((val === undefined || val === "all") && rest.length === 0) {
        searchParams.delete(SearchParamsKeys.LOG_LEVEL);
      } else {
        searchParams.delete(SearchParamsKeys.LOG_LEVEL);
        value
          .filter((state) => state !== "all")
          .map((state) => searchParams.append(SearchParamsKeys.LOG_LEVEL, state));
      }
      setSearchParams(searchParams);
    },
    [searchParams, setSearchParams],
  );

  return (
    <Box>
      {taskInstance === undefined || tryNumber === undefined || taskInstance.try_number <= 1 ? undefined : (
        <TaskTrySelect
          onSelectTryNumber={onSelectTryNumber}
          selectedTryNumber={tryNumber}
          taskInstance={taskInstance}
        />
      )}
      <HStack justifyContent="space-between" mb={2}>
        <Select.Root
          collection={logLevelOptions}
          maxW="250px"
          multiple
          onValueChange={handleStateChange}
          value={hasLogLevels ? logLevels : ["all"]}
        >
          <Select.Trigger {...(hasLogLevels ? { clearable: true } : {})} isActive={Boolean(logLevels)}>
            <Select.ValueText>
              {() =>
                hasLogLevels ? (
                  <HStack gap="10px">
                    {logLevels.map((level) => (
                      <Badge colorPalette={logLevelColorMapping[level as LogLevel]} key={level}>
                        {level.toUpperCase()}
                      </Badge>
                    ))}
                  </HStack>
                ) : (
                  "All Log Levels"
                )
              }
            </Select.ValueText>
          </Select.Trigger>
          <Select.Content>
            {logLevelOptions.items.map((option) => (
              <Select.Item item={option} key={option.label}>
                {option.value === "all" ? (
                  option.label
                ) : (
                  <Badge colorPalette={logLevelColorMapping[option.value as LogLevel]}>{option.label}</Badge>
                )}
              </Select.Item>
            ))}
          </Select.Content>
        </Select.Root>
        <HStack>
          <Button aria-label={wrap ? "Unwrap" : "Wrap"} bg="bg.panel" onClick={toggleWrap} variant="outline">
            {wrap ? "Unwrap" : "Wrap"}
          </Button>
          {!isFullscreen && (
            <IconButton aria-label="Full screen" bg="bg.panel" onClick={toggleFullscreen} variant="outline">
              <MdOutlineOpenInFull />
            </IconButton>
          )}
        </HStack>
      </HStack>
    </Box>
  );
};
