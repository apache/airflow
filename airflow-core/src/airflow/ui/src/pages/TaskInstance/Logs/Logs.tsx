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
import { useState } from "react";

import { Box, Button, Heading } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { useParams, useSearchParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import { useTaskInstanceServiceGetMappedTaskInstance } from "openapi/queries";

import { Alert, Modal } from "src/system-components";

import { TaskTrySelect } from "src/components/TaskTrySelect";

import {
  LOG_SHOW_LOG_LEVEL_KEY,
  LOG_SHOW_SOURCE_KEY,
  LOG_SHOW_TIMESTAMP_KEY,
  LOG_WRAP_KEY,
} from "src/constants/localStorage";
import { SearchParamsKeys } from "src/constants/searchParams";
import { SHORTCUTS } from "src/context/keyboardShortcuts";
import { useShortcut } from "src/hooks/useShortcut";
import { useConfig } from "src/queries/useConfig";
import { useLogs } from "src/queries/useLogs";

import { ExternalLogLink } from "./ExternalLogLink";
import { TaskLogContent, type TaskLogContentProps } from "./TaskLogContent";
import { TaskLogHeader, type TaskLogHeaderProps } from "./TaskLogHeader";
import { getDownloadText } from "./utils";

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

  const defaultTryNumber = taskInstance?.try_number;

  const onSelectTryNumber = (newTryNumber: number) => {
    if (newTryNumber === defaultTryNumber) {
      searchParams.delete(SearchParamsKeys.TRY_NUMBER);
    } else {
      searchParams.set(SearchParamsKeys.TRY_NUMBER, newTryNumber.toString());
    }
    setSearchParams(searchParams);
  };

  const tryNumber = tryNumberParam === null ? defaultTryNumber : parseInt(tryNumberParam, 10);

  const isPendingTry =
    taskInstance !== undefined &&
    tryNumber === taskInstance.try_number &&
    (taskInstance.state === null || taskInstance.state === "up_for_retry");

  const defaultWrap = Boolean(useConfig("default_wrap"));

  const [wrap, setWrap] = useLocalStorage<boolean>(LOG_WRAP_KEY, defaultWrap);
  const [showTimestamp, setShowTimestamp] = useLocalStorage<boolean>(LOG_SHOW_TIMESTAMP_KEY, true);
  const [showSource, setShowSource] = useLocalStorage<boolean>(LOG_SHOW_SOURCE_KEY, false);
  const [showLogLevel, setShowLogLevel] = useLocalStorage<boolean>(LOG_SHOW_LOG_LEVEL_KEY, true);
  const [fullscreen, setFullscreen] = useState(false);
  const [expanded, setExpanded] = useState(false);

  const {
    error: logError,
    fetchedData,
    isLoading: isLoadingLogs,
    parsedData,
  } = useLogs(
    {
      dagId,
      logLevelFilters,
      showLogLevel,
      showSource,
      showTimestamp,
      sourceFilters,
      taskInstance,
      tryNumber,
    },
    { enabled: Boolean(taskInstance) && !isPendingTry },
  );

  const downloadTextLines = getDownloadText({
    fetchedData,
    logLevelFilters,
    showLogLevel,
    showSource,
    showTimestamp,
    sourceFilters,
    translate,
  });

  const getLogString = () => downloadTextLines.filter((line) => line !== "").join("\n");

  const [searchQuery, setSearchQuery] = useState("");
  const [activeSearchIndex, setActiveSearchIndex] = useState(0);

  const searchMatchIndices = (() => {
    if (!searchQuery) {
      return [];
    }
    const query = searchQuery.toLowerCase();
    const indices: Array<number> = [];

    parsedData.searchableText.forEach((line, index) => {
      if (line.toLowerCase().includes(query)) {
        indices.push(index);
      }
    });

    return indices;
  })();

  const handleSearchChange = (query: string) => {
    setSearchQuery(query);
    setActiveSearchIndex(0);
  };

  const handleSearchNext = () => {
    if (searchMatchIndices.length > 0) {
      setActiveSearchIndex((prev) => (prev + 1) % searchMatchIndices.length);
    }
  };

  const handleSearchPrevious = () => {
    if (searchMatchIndices.length > 0) {
      setActiveSearchIndex((prev) => (prev - 1 + searchMatchIndices.length) % searchMatchIndices.length);
    }
  };

  const downloadLogs = () => {
    const logContent = getLogString();
    const element = document.createElement("a");

    element.href = URL.createObjectURL(new Blob([logContent], { type: "text/plain" }));
    element.download = `logs_${taskInstance?.dag_id}_${taskInstance?.dag_run_id}_${taskInstance?.task_id}_${taskInstance?.map_index}_${tryNumber}.txt`;
    document.body.append(element);
    element.click();
    element.remove();
  };

  const toggleWrap = () => setWrap(!wrap);
  const toggleTimestamp = () => setShowTimestamp(!showTimestamp);
  const toggleLogLevel = () => setShowLogLevel(!showLogLevel);
  const toggleSource = () => setShowSource(!showSource);
  const toggleFullscreen = () => setFullscreen(!fullscreen);
  const toggleExpanded = () => setExpanded((act) => !act);

  useShortcut({
    ...SHORTCUTS.logs.toggleWrap,
    callback: toggleWrap,
    options: { enabled: !isPendingTry },
  });
  useShortcut({
    ...SHORTCUTS.logs.toggleFullscreen,
    callback: toggleFullscreen,
    options: { enabled: !isPendingTry },
  });
  useShortcut({
    ...SHORTCUTS.logs.toggleExpand,
    callback: toggleExpanded,
    options: { enabled: !isPendingTry },
  });
  useShortcut({
    ...SHORTCUTS.logs.toggleTimestamp,
    callback: toggleTimestamp,
    options: { enabled: !isPendingTry },
  });
  useShortcut({
    ...SHORTCUTS.logs.toggleLogLevel,
    callback: toggleLogLevel,
    options: { enabled: !isPendingTry },
  });
  useShortcut({
    ...SHORTCUTS.logs.toggleSource,
    callback: toggleSource,
    options: { enabled: !isPendingTry },
  });
  useShortcut({
    ...SHORTCUTS.logs.downloadLogs,
    callback: downloadLogs,
    options: { enabled: !isPendingTry },
  });

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
    search: {
      currentMatchIndex: activeSearchIndex,
      onSearchChange: handleSearchChange,
      onSearchNext: handleSearchNext,
      onSearchPrevious: handleSearchPrevious,
      searchQuery,
      totalMatches: searchMatchIndices.length,
    },
    showLogLevel,
    showSource,
    showTimestamp,
    sourceOptions: parsedData.sources,
    taskInstance,
    toggleExpanded,
    toggleFullscreen,
    toggleLogLevel,
    toggleSource,
    toggleTimestamp,
    toggleWrap,
    tryNumber,
    wrap,
  };

  const logContentProps: TaskLogContentProps = {
    currentMatchLineIndex: searchMatchIndices[activeSearchIndex],
    error,
    expanded,
    isLoading: isLoading || isLoadingLogs,
    logError,
    parsedLogs: parsedData.parsedLogs ?? [],
    searchMatchIndices: searchQuery ? new Set(searchMatchIndices) : undefined,
    searchQuery: searchQuery || undefined,
    wrap,
  };

  if (isPendingTry) {
    const isRetry = taskInstance.state === "up_for_retry";

    return (
      <Box p={2}>
        {taskInstance.try_number > 1 ? (
          <TaskTrySelect
            onSelectTryNumber={onSelectTryNumber}
            selectedTryNumber={tryNumber}
            taskInstance={taskInstance}
          />
        ) : undefined}
        <Alert
          status="info"
          title={translate(isRetry ? "logs.waitingToRetry" : "logs.tryNotStarted", { tryNumber })}
        >
          {isRetry ? translate("logs.tryNotStarted") : undefined}
          {taskInstance.try_number > 1 ? (
            <Box mt={3}>
              <Button onClick={() => onSelectTryNumber(taskInstance.try_number - 1)} variant="outline">
                {translate(isRetry ? "logs.viewFailedTry" : "logs.viewPreviousTry", {
                  tryNumber: taskInstance.try_number - 1,
                })}
              </Button>
            </Box>
          ) : undefined}
        </Alert>
      </Box>
    );
  }

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
      <Modal
        bodyProps={{ display: "flex", flexDirection: "column" }}
        headerProps={{
          children: (
            <Box display="flex" flexDirection="column" width="100%">
              <Heading mb={2} size="xl">
                {taskId}
              </Heading>
              <TaskLogHeader {...logHeaderProps} isFullscreen />
            </Box>
          ),
          width: "100%",
        }}
        onOpenChange={onOpenChange}
        open={fullscreen}
        scrollBehavior="inside"
        size="full"
      >
        <TaskLogContent {...logContentProps} />
      </Modal>
    </Box>
  );
};
