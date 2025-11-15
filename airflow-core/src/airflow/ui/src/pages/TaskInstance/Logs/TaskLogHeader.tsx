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
import { useTranslation } from "react-i18next";
import {
  MdAccessTime,
  MdCode,
  MdCompress,
  MdExpand,
  MdOutlineOpenInFull,
  MdSettings,
  MdWrapText,
  MdOutlineFileDownload,
} from "react-icons/md";
import { useSearchParams } from "react-router-dom";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { TaskTrySelect } from "src/components/TaskTrySelect";
import { Button, Menu, Select, Tooltip } from "src/components/ui";
import { SearchParamsKeys } from "src/constants/searchParams";
import { createTheme } from "src/theme";
import { type LogLevel, logLevelColorMapping, logLevelOptions } from "src/utils/logs";

type Props = {
  readonly downloadLogs?: () => void;
  readonly expanded?: boolean;
  readonly isFullscreen?: boolean;
  readonly onSelectTryNumber: (tryNumber: number) => void;
  readonly showSource: boolean;
  readonly showTimestamp: boolean;
  readonly sourceOptions?: Array<string>;
  readonly taskInstance?: TaskInstanceResponse;
  readonly toggleExpanded?: () => void;
  readonly toggleFullscreen: () => void;
  readonly toggleSource: () => void;
  readonly toggleTimestamp: () => void;
  readonly toggleWrap: () => void;
  readonly tryNumber?: number;
  readonly wrap: boolean;
};

export const TaskLogHeader = ({
  downloadLogs,
  expanded,
  isFullscreen = false,
  onSelectTryNumber,
  showSource,
  showTimestamp,
  sourceOptions,
  taskInstance,
  toggleExpanded,
  toggleFullscreen,
  toggleSource,
  toggleTimestamp,
  toggleWrap,
  tryNumber,
  wrap,
}: Props) => {
  const { t: translate } = useTranslation(["common", "dag"]);
  const [searchParams, setSearchParams] = useSearchParams();
  const sources = searchParams.getAll(SearchParamsKeys.SOURCE);
  const logLevels = searchParams.getAll(SearchParamsKeys.LOG_LEVEL);
  const hasLogLevels = logLevels.length > 0;
  const system = createTheme();

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
      { label: translate("dag:logs.allSources"), value: "all" },
      ...(sourceOptions ?? []).map((source) => ({ label: source, value: source })),
    ],
  });

  const handleLevelChange = useCallback(
    ({ value }: SelectValueChangeDetails<string>) => {
      const [val, ...rest] = value;

      if (((val === undefined || val === "all") && rest.length === 0) || rest.includes("all")) {
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

      if (((val === undefined || val === "all") && rest.length === 0) || rest.includes("all")) {
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
      <HStack justifyContent="space-between">
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
                  translate("dag:logs.allLevels")
                )
              }
            </Select.ValueText>
          </Select.Trigger>
          <Select.Content zIndex={zIndex}>
            {logLevelOptions.items.map((option) => (
              <Select.Item item={option} key={option.label}>
                {option.value === "all" ? (
                  translate(option.label)
                ) : (
                  <Badge colorPalette={logLevelColorMapping[option.value as LogLevel]}>
                    {translate(option.label)}
                  </Badge>
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
              <Select.ValueText placeholder={translate("dag:logs.allSources")} />
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
        <HStack gap={1}>
          <Menu.Root>
            <Menu.Trigger asChild>
              <Button variant="outline">
                <MdSettings /> {translate("dag:logs.settings")}
              </Button>
            </Menu.Trigger>
            <Menu.Content zIndex={zIndex}>
              <Menu.Item onClick={toggleWrap} value="wrap">
                <MdWrapText /> {wrap ? translate("wrap.unwrap") : translate("wrap.wrap")}
                <Menu.ItemCommand>{translate("wrap.hotkey")}</Menu.ItemCommand>
              </Menu.Item>
              <Menu.Item onClick={toggleTimestamp} value="timestamp">
                <MdAccessTime /> {showTimestamp ? translate("timestamp.hide") : translate("timestamp.show")}
                <Menu.ItemCommand>{translate("timestamp.hotkey")}</Menu.ItemCommand>
              </Menu.Item>
              <Menu.Item onClick={toggleExpanded} value="expand">
                {expanded ? (
                  <>
                    <MdCompress /> {translate("expand.collapse")}
                  </>
                ) : (
                  <>
                    <MdExpand /> {translate("expand.expand")}
                  </>
                )}
                <Menu.ItemCommand>{translate("expand.hotkey")}</Menu.ItemCommand>
              </Menu.Item>
              <Menu.Item onClick={toggleSource} value="source">
                <MdCode /> {showSource ? translate("source.hide") : translate("source.show")}
                <Menu.ItemCommand>{translate("source.hotkey")}</Menu.ItemCommand>
              </Menu.Item>
            </Menu.Content>
          </Menu.Root>
          {!isFullscreen && (
            <Tooltip
              closeDelay={100}
              content={translate("dag:logs.fullscreen.tooltip", { hotkey: "f" })}
              openDelay={100}
            >
              <IconButton
                aria-label={translate("dag:logs.fullscreen.button")}
                bg="bg.panel"
                m={0}
                onClick={toggleFullscreen}
                px={4}
                py={2}
                variant="outline"
              >
                <MdOutlineOpenInFull />
              </IconButton>
            </Tooltip>
          )}

          <Tooltip closeDelay={100} content={translate("download.tooltip", { hotkey: "d" })} openDelay={100}>
            <IconButton
              aria-label={translate("download.download")}
              bg="bg.panel"
              m={0}
              onClick={downloadLogs}
              px={4}
              py={2}
              variant="outline"
            >
              <MdOutlineFileDownload />
            </IconButton>
          </Tooltip>
        </HStack>
      </HStack>
    </Box>
  );
};
