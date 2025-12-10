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
import { Box, CloseButton, Dialog, Heading, VStack } from "@chakra-ui/react";
import { useState } from "react";
import { useHotkeys } from "react-hotkeys-hook";
import { useTranslation } from "react-i18next";
import { useParams, useSearchParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import { useTaskInstanceServiceGetMappedTaskInstance } from "openapi/queries";
import { renderStructuredLog } from "src/components/renderStructuredLog";
import { SearchParamsKeys } from "src/constants/searchParams";
import { useConfig } from "src/queries/useConfig";
import { useLogs } from "src/queries/useLogs";
import { parseStreamingLogContent } from "src/utils/logs";

import { ExternalLogLink } from "./ExternalLogLink";
import { TaskLogContent } from "./TaskLogContent";
import { TaskLogHeader } from "./TaskLogHeader";

export const Logs = () => {
  const { dagId = "", mapIndex = "-1", runId = "", taskId = "" } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const { t: translate } = useTranslation("dag");

  const tryNumberParam = searchParams.get(SearchParamsKeys.TRY_NUMBER);
  const logLevelFilters = searchParams.getAll(SearchParamsKeys.LOG_LEVEL);
  const sourceFilters = searchParams.getAll(SearchParamsKeys.SOURCE);
  const parsedMapIndex = parseInt(mapIndex, 10);

  const {
    data: taskInstance,
    error,
    isLoading,
  } = useTaskInstanceServiceGetMappedTaskInstance(
    {
      dagId,
      dagRunId: runId,
      mapIndex: parsedMapIndex,
      taskId,
    },
    undefined,
    {
      enabled: !isNaN(parsedMapIndex),
    },
  );

  const onSelectTryNumber = (newTryNumber: number) => {
    if (newTryNumber === taskInstance?.try_number) {
      searchParams.delete(SearchParamsKeys.TRY_NUMBER);
    } else {
      searchParams.set(SearchParamsKeys.TRY_NUMBER, newTryNumber.toString());
    }
    setSearchParams(searchParams);
  };

  const tryNumber = tryNumberParam === null ? taskInstance?.try_number : parseInt(tryNumberParam, 10);

  const defaultWrap = Boolean(useConfig("default_wrap"));
  const defaultShowTimestamp = Boolean(true);

  const [wrap, setWrap] = useLocalStorage<boolean>("log_wrap", defaultWrap);
  const [showTimestamp, setShowTimestamp] = useLocalStorage<boolean>(
    "log_show_timestamp",
    defaultShowTimestamp,
  );
  const [showSource, setShowSource] = useLocalStorage<boolean>("log_show_source", false);
  const [fullscreen, setFullscreen] = useState(false);
  const [expanded, setExpanded] = useState(false);

  const {
    error: logError,
    fetchedData,
    isLoading: isLoadingLogs,
    parsedData,
  } = useLogs({
    dagId,
    expanded,
    logLevelFilters,
    showSource,
    showTimestamp,
    sourceFilters,
    taskInstance,
    tryNumber,
  });

  const downloadLogs = () => {
    const lines = parseStreamingLogContent(fetchedData);
    const parsedLines = lines.map((line) =>
      renderStructuredLog({
        index: 0,
        logLevelFilters,
        logLink: "",
        logMessage: line,
        renderingMode: "text",
        showSource,
        showTimestamp,
        sourceFilters,
        translate,
      }),
    );

    const logContent = parsedLines.join("\n");
    const element = document.createElement("a");

    element.href = URL.createObjectURL(new Blob([logContent], { type: "text/plain" }));
    element.download = `logs_${taskInstance?.dag_id}_${taskInstance?.dag_run_id}_${taskInstance?.task_id}_${taskInstance?.map_index}_${taskInstance?.try_number}.txt`;
    document.body.append(element);
    element.click();
    element.remove();
  };

  const toggleWrap = () => setWrap(!wrap);
  const toggleTimestamp = () => setShowTimestamp(!showTimestamp);
  const toggleSource = () => setShowSource(!showSource);
  const toggleFullscreen = () => setFullscreen(!fullscreen);
  const toggleExpanded = () => setExpanded((act) => !act);

  useHotkeys("w", toggleWrap);
  useHotkeys("f", toggleFullscreen);
  useHotkeys("e", toggleExpanded);
  useHotkeys("t", toggleTimestamp);
  useHotkeys("s", toggleSource);
  useHotkeys("d", downloadLogs);

  const onOpenChange = () => {
    setFullscreen(false);
  };

  const externalLogName = useConfig("external_log_name") as string;
  const showExternalLogRedirect = Boolean(useConfig("show_external_log_redirect"));

  return (
    <Box display="flex" flexDirection="column" h="100%" p={2}>
      <TaskLogHeader
        downloadLogs={downloadLogs}
        expanded={expanded}
        onSelectTryNumber={onSelectTryNumber}
        showSource={showSource}
        showTimestamp={showTimestamp}
        sourceOptions={parsedData.sources}
        taskInstance={taskInstance}
        toggleExpanded={toggleExpanded}
        toggleFullscreen={toggleFullscreen}
        toggleSource={toggleSource}
        toggleTimestamp={toggleTimestamp}
        toggleWrap={toggleWrap}
        tryNumber={tryNumber}
        wrap={wrap}
      />
      {showExternalLogRedirect && externalLogName && taskInstance ? (
        tryNumber === undefined ? (
          <p>{translate("logs.noTryNumber")}</p>
        ) : (
          <ExternalLogLink
            externalLogName={externalLogName}
            taskInstance={taskInstance}
            tryNumber={tryNumber}
          />
        )
      ) : undefined}
      <TaskLogContent
        error={error}
        isLoading={isLoading || isLoadingLogs}
        logError={logError}
        parsedLogs={parsedData.parsedLogs ?? []}
        wrap={wrap}
      />
      <Dialog.Root onOpenChange={onOpenChange} open={fullscreen} scrollBehavior="inside" size="full">
        <Dialog.Backdrop />
        <Dialog.Positioner>
          <Dialog.Content>
            <Dialog.Header>
              <VStack alignItems="flex-start" gap={2}>
                <Heading size="xl">{taskId}</Heading>
                <TaskLogHeader
                  downloadLogs={downloadLogs}
                  expanded={expanded}
                  isFullscreen
                  onSelectTryNumber={onSelectTryNumber}
                  showSource={showSource}
                  showTimestamp={showTimestamp}
                  taskInstance={taskInstance}
                  toggleExpanded={toggleExpanded}
                  toggleFullscreen={toggleFullscreen}
                  toggleSource={toggleSource}
                  toggleTimestamp={toggleTimestamp}
                  toggleWrap={toggleWrap}
                  tryNumber={tryNumber}
                  wrap={wrap}
                />
              </VStack>
            </Dialog.Header>

            <Dialog.CloseTrigger asChild position="absolute" right="2" top="2">
              <CloseButton size="sm" />
            </Dialog.CloseTrigger>

            <Dialog.Body display="flex" flexDirection="column">
              <TaskLogContent
                error={error}
                isLoading={isLoading || isLoadingLogs}
                logError={logError}
                parsedLogs={parsedData.parsedLogs ?? []}
                wrap={wrap}
              />
            </Dialog.Body>
          </Dialog.Content>
        </Dialog.Positioner>
      </Dialog.Root>
    </Box>
  );
};
