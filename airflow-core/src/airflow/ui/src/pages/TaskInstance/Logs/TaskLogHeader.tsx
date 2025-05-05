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
import { Button, Select, Tooltip } from "src/components/ui";
import { SearchParamsKeys } from "src/constants/searchParams";
import { system } from "src/theme";
import { type LogLevel, logLevelColorMapping, logLevelOptions } from "src/utils/logs";

type Props = {
  readonly isFullscreen?: boolean;
  readonly onSelectTryNumber: (tryNumber: number) => void;
  readonly sourceOptions?: Array<string>;
  readonly taskInstance?: TaskInstanceResponse;
  readonly toggleFullscreen: () => void;
  readonly toggleWrap: () => void;
  readonly tryNumber?: number;
  readonly wrap: boolean;
};

export const TaskLogHeader = ({
  isFullscreen = false,
  onSelectTryNumber,
  sourceOptions,
  taskInstance,
  toggleFullscreen,
  toggleWrap,
  tryNumber,
  wrap,
}: Props) => {
  const [searchParams, setSearchParams] = useSearchParams();
  const sources = searchParams.getAll(SearchParamsKeys.SOURCE);
  const logLevels = searchParams.getAll(SearchParamsKeys.LOG_LEVEL);
  const hasLogLevels = logLevels.length > 0;

  // Have select zIndex greater than modal zIndex in fullscreen so that
  // select options are displayed.
  const zIndex = isFullscreen
    ? Number(system.tokens.categoryMap.get("zIndex")?.get("modal")?.value ?? 1400) + 1
    : undefined;

  const sourceOptionList = createListCollection<{
    label: string;
    value: string;
  }>({
    items: [
      { label: "All Sources", value: "all" },
      ...(sourceOptions ?? []).map((source) => ({ label: source, value: source })),
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

  const handleSourceChange = useCallback(
    ({ value }: SelectValueChangeDetails<string>) => {
      const [val, ...rest] = value;

      if ((val === undefined || val === "all") && rest.length === 0) {
        searchParams.delete(SearchParamsKeys.SOURCE);
      } else {
        searchParams.delete(SearchParamsKeys.SOURCE);
        value
          .filter((state) => state !== "all")
          .map((state) => searchParams.append(SearchParamsKeys.SOURCE, state));
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
          minW={isFullscreen ? "150px" : undefined}
          multiple
          onValueChange={handleLevelChange}
          value={hasLogLevels ? logLevels : ["all"]}
        >
          <Select.Trigger {...(hasLogLevels ? { clearable: true } : {})} isActive={Boolean(logLevels)}>
            <Select.ValueText>
              {() =>
                hasLogLevels ? (
                  <HStack flexWrap="wrap" fontSize="sm" gap="4px" paddingY="8px">
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
          <Select.Content zIndex={zIndex}>
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
        {sourceOptions !== undefined && sourceOptions.length > 0 ? (
          <Select.Root
            collection={sourceOptionList}
            maxW="250px"
            multiple
            onValueChange={handleSourceChange}
            value={sources}
          >
            <Select.Trigger clearable>
              <Select.ValueText placeholder="All Sources" />
            </Select.Trigger>
            <Select.Content>
              {sourceOptionList.items.map((option) => (
                <Select.Item item={option} key={option.label}>
                  {option.label}
                </Select.Item>
              ))}
            </Select.Content>
          </Select.Root>
        ) : undefined}
        <HStack>
          <Tooltip closeDelay={100} content="Press w to toggle wrap" openDelay={100}>
            <Button
              aria-label={wrap ? "Unwrap" : "Wrap"}
              bg="bg.panel"
              onClick={toggleWrap}
              variant="outline"
            >
              {wrap ? "Unwrap" : "Wrap"}
            </Button>
          </Tooltip>
          {!isFullscreen && (
            <Tooltip closeDelay={100} content="Press f for fullscreen" openDelay={100}>
              <IconButton aria-label="Full screen" bg="bg.panel" onClick={toggleFullscreen} variant="outline">
                <MdOutlineOpenInFull />
              </IconButton>
            </Tooltip>
          )}
        </HStack>
      </HStack>
    </Box>
  );
};
