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
import { Box, Heading } from "@chakra-ui/react";
import { useState } from "react";
import { useHotkeys } from "react-hotkeys-hook";
import { useTranslation } from "react-i18next";
import { useParams, useSearchParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import { useTaskInstanceServiceGetMappedTaskInstance } from "openapi/queries";
import { renderStructuredLog } from "src/components/renderStructuredLog";
import { Dialog } from "src/components/ui";
import { LOG_SHOW_SOURCE_KEY, LOG_SHOW_TIMESTAMP_KEY, LOG_WRAP_KEY } from "src/constants/localStorage";
import { SearchParamsKeys } from "src/constants/searchParams";
import { useConfig } from "src/queries/useConfig";
import { useLogs } from "src/queries/useLogs";
import { parseStreamingLogContent } from "src/utils/logs";

import { ExternalLogLink } from "./ExternalLogLink";
import { TaskLogContent, type TaskLogContentProps } from "./TaskLogContent";
import { TaskLogHeader, type TaskLogHeaderProps } from "./TaskLogHeader";

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

  const [wrap, setWrap] = useLocalStorage<boolean>(LOG_WRAP_KEY, defaultWrap);
  const [showTimestamp, setShowTimestamp] = useLocalStorage<boolean>(LOG_SHOW_TIMESTAMP_KEY, true);
  const [showSource, setShowSource] = useLocalStorage<boolean>(LOG_SHOW_SOURCE_KEY, false);
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

  const getParsedLogs = () => {
    const lines = parseStreamingLogContent(fetchedData);

    return lines.map((line) =>
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
  };

  const getLogString = () => getParsedLogs().join("\n");

  const downloadLogs = () => {
    const logContent = getLogString();
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

  const logHeaderProps: TaskLogHeaderProps = {
    downloadLogs,
    expanded,
    getLogString,
    onSelectTryNumber,
    showSource,
    showTimestamp,
    sourceOptions: parsedData.sources,
    taskInstance,
    toggleExpanded,
    toggleFullscreen,
    toggleSource,
    toggleTimestamp,
    toggleWrap,
    tryNumber,
    wrap,
  };

  const logContentProps: TaskLogContentProps = {
    error,
    isLoading: isLoading || isLoadingLogs,
    logError,
    parsedLogs: parsedData.parsedLogs ?? [],
    wrap,
  };

  return (
    <Box display="flex" flexDirection="column" h="100%" p={2}>
      <TaskLogHeader {...logHeaderProps} />
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
      <TaskLogContent {...logContentProps} />
      <Dialog.Root onOpenChange={onOpenChange} open={fullscreen} scrollBehavior="inside" size="full">
        {fullscreen ? (
          <Dialog.Content backdrop>
            <Dialog.Header width="100%">
              <Box display="flex" flexDirection="column" width="100%">
                <Heading mb={2} size="xl">
                  {taskId}
                </Heading>
                <TaskLogHeader {...logHeaderProps} />
              </Box>
            </Dialog.Header>

            <Dialog.CloseTrigger />

            <Dialog.Body display="flex" flexDirection="column">
              <TaskLogContent {...logContentProps} />
            </Dialog.Body>
          </Dialog.Content>
        ) : undefined}
      </Dialog.Root>
    </Box>
  );
};
