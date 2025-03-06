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
import {
  Badge,
  Box,
  createListCollection,
  HStack,
  IconButton,
  type SelectValueChangeDetails,
} from "@chakra-ui/react";
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
  readonly loggerOptions?: Array<string>;
  readonly onSelectTryNumber: (tryNumber: number) => void;
  readonly taskInstance?: TaskInstanceResponse;
  readonly toggleFullscreen: () => void;
  readonly toggleWrap: () => void;
  readonly tryNumber?: number;
  readonly wrap: boolean;
};

export const TaskLogHeader = ({
  isFullscreen = false,
  loggerOptions,
  onSelectTryNumber,
  taskInstance,
  toggleFullscreen,
  toggleWrap,
  tryNumber,
  wrap,
}: Props) => {
  const [searchParams, setSearchParams] = useSearchParams();
  const loggers = searchParams.getAll(SearchParamsKeys.LOGGER);
  const logLevels = searchParams.getAll(SearchParamsKeys.LOG_LEVEL);
  const hasLogLevels = logLevels.length > 0;

  const loggerOptionList = createListCollection<{
    label: string;
    value: string;
  }>({
    items: [
      { label: "All Loggers", value: "all" },
      ...(loggerOptions ?? []).map((logger) => ({ label: logger, value: logger })),
    ],
  });

  const handleLevelChange = useCallback(
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

  const handleLoggerChange = useCallback(
    ({ value }: SelectValueChangeDetails<string>) => {
      const [val, ...rest] = value;

      if ((val === undefined || val === "all") && rest.length === 0) {
        searchParams.delete(SearchParamsKeys.LOGGER);
      } else {
        searchParams.delete(SearchParamsKeys.LOGGER);
        value
          .filter((state) => state !== "all")
          .map((state) => searchParams.append(SearchParamsKeys.LOGGER, state));
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
          onValueChange={handleLevelChange}
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
        {loggerOptions !== undefined && loggerOptions.length > 0 ? (
          <Select.Root
            collection={loggerOptionList}
            maxW="250px"
            multiple
            onValueChange={handleLoggerChange}
            value={loggers}
          >
            <Select.Trigger clearable>
              <Select.ValueText placeholder="All Loggers" />
            </Select.Trigger>
            <Select.Content>
              {loggerOptionList.items.map((option) => (
                <Select.Item item={option} key={option.label}>
                  {option.label}
                </Select.Item>
              ))}
            </Select.Content>
          </Select.Root>
        ) : undefined}
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
