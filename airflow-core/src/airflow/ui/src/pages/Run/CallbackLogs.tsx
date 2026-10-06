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

import { Badge, Box, Heading, HStack, Link, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiAlertTriangle, FiArrowLeft, FiClock } from "react-icons/fi";
import { Link as RouterLink, useParams, useSearchParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import {
  useDagRunServiceGetDagRun,
  useDeadlinesServiceGetDagDeadlineAlerts,
  useDeadlinesServiceGetDeadlines,
} from "openapi/queries";

import { Modal } from "src/system-components";

import { TaskLogContent, type TaskLogContentProps } from "src/pages/TaskInstance/Logs/TaskLogContent";
import { TaskLogHeader, type TaskLogHeaderProps } from "src/pages/TaskInstance/Logs/TaskLogHeader";
import { getDownloadText } from "src/pages/TaskInstance/Logs/utils";

import Time from "src/components/Time";

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
import { useCallbackLogs } from "src/queries/useLogs";
import { useDurationFormat } from "src/utils";
import { translateCompletionRule } from "src/utils/deadlines";

import { CallbackStateBadge, getMissedBy, translateCallbackType } from "./Callbacks";

export const CallbackLogs = () => {
  const { callbackId = "", dagId = "", runId = "" } = useParams();
  const [searchParams] = useSearchParams();
  const { t: translate } = useTranslation("dag");

  const logLevelFilters = searchParams.getAll(SearchParamsKeys.LOG_LEVEL);
  const sourceFilters = searchParams.getAll(SearchParamsKeys.SOURCE);

  const defaultWrap = Boolean(useConfig("default_wrap"));

  const [wrap, setWrap] = useLocalStorage<boolean>(LOG_WRAP_KEY, defaultWrap);
  const [showTimestamp, setShowTimestamp] = useLocalStorage<boolean>(LOG_SHOW_TIMESTAMP_KEY, true);
  const [showSource, setShowSource] = useLocalStorage<boolean>(LOG_SHOW_SOURCE_KEY, false);
  const [showLogLevel, setShowLogLevel] = useLocalStorage<boolean>(LOG_SHOW_LOG_LEVEL_KEY, true);
  const [fullscreen, setFullscreen] = useState(false);
  const [expanded, setExpanded] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [activeSearchIndex, setActiveSearchIndex] = useState(0);

  // Same params as the Callbacks tab's first page, so this is usually served from its cache.
  const pageSize = useConfig("fallback_page_limit") as number;
  const { data: deadlines } = useDeadlinesServiceGetDeadlines({
    dagId,
    dagRunId: runId,
    limit: pageSize,
    offset: 0,
  });
  const deadline = deadlines?.deadlines.find((dl) => dl.callback_id === callbackId);
  const { data: dagRun } = useDagRunServiceGetDagRun({ dagId, dagRunId: runId });
  const { locale, renderDuration } = useDurationFormat();
  // Same params as the run header's deadline badge.
  const { data: alertData } = useDeadlinesServiceGetDagDeadlineAlerts({ dagId, limit: 100 });
  const alert = alertData?.deadline_alerts.find(({ id }) => id === deadline?.alert_id);
  // Skipped for a dynamic interval, whose rule would not name a length.
  const completionRule =
    alert?.interval === null || alert?.interval === undefined
      ? undefined
      : translateCompletionRule(translate, alert, locale);

  const { error, fetchedData, isLoading, parsedData } = useCallbackLogs({
    callbackId,
    dagId,
    dagRunId: runId,
    logLevelFilters,
    showLogLevel,
    showSource,
    showTimestamp,
    sourceFilters,
  });

  const getLogString = () =>
    getDownloadText({
      fetchedData,
      logLevelFilters,
      showLogLevel,
      showSource,
      showTimestamp,
      sourceFilters,
      translate,
    })
      .filter((line) => line !== "")
      .join("\n");

  const searchMatchIndices = searchQuery
    ? parsedData.searchableText.flatMap((line, index) =>
        line.toLowerCase().includes(searchQuery.toLowerCase()) ? [index] : [],
      )
    : [];

  const downloadLogs = () => {
    const element = document.createElement("a");

    element.href = URL.createObjectURL(new Blob([getLogString()], { type: "text/plain" }));
    element.download = `logs_${dagId}_${runId}_callback_${callbackId}.txt`;
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

  useShortcut({ ...SHORTCUTS.logs.toggleWrap, callback: toggleWrap });
  useShortcut({ ...SHORTCUTS.logs.toggleFullscreen, callback: toggleFullscreen });
  useShortcut({ ...SHORTCUTS.logs.toggleExpand, callback: toggleExpanded });
  useShortcut({ ...SHORTCUTS.logs.toggleTimestamp, callback: toggleTimestamp });
  useShortcut({ ...SHORTCUTS.logs.toggleLogLevel, callback: toggleLogLevel });
  useShortcut({ ...SHORTCUTS.logs.toggleSource, callback: toggleSource });
  useShortcut({ ...SHORTCUTS.logs.downloadLogs, callback: downloadLogs });

  const logHeaderProps: TaskLogHeaderProps = {
    downloadLogs,
    expanded,
    getLogString,
    onSelectTryNumber: () => undefined,
    search: {
      currentMatchIndex: activeSearchIndex,
      onSearchChange: (query: string) => {
        setSearchQuery(query);
        setActiveSearchIndex(0);
      },
      onSearchNext: () => setActiveSearchIndex((prev) => (prev + 1) % Math.max(searchMatchIndices.length, 1)),
      onSearchPrevious: () =>
        setActiveSearchIndex(
          (prev) => (prev - 1 + searchMatchIndices.length) % Math.max(searchMatchIndices.length, 1),
        ),
      searchQuery,
      totalMatches: searchMatchIndices.length,
    },
    showLogLevel,
    showSource,
    showTimestamp,
    sourceOptions: parsedData.sources,
    toggleExpanded,
    toggleFullscreen,
    toggleLogLevel,
    toggleSource,
    toggleTimestamp,
    toggleWrap,
    wrap,
  };

  const logContentProps: TaskLogContentProps = {
    currentMatchLineIndex: searchMatchIndices[activeSearchIndex],
    error,
    expanded,
    isLoading,
    logError: error,
    parsedLogs: parsedData.parsedLogs ?? [],
    searchMatchIndices: searchQuery ? new Set(searchMatchIndices) : undefined,
    searchQuery: searchQuery || undefined,
    wrap,
  };

  return (
    <Box display="flex" flexDirection="column" h="100%" p={2}>
      <Link asChild color="fg.info" fontSize="sm" mb={2}>
        <RouterLink to={`/dags/${dagId}/runs/${runId}/callbacks`}>
          <FiArrowLeft />
          {translate("callbacks.allCallbacks")}
        </RouterLink>
      </Link>
      {deadline === undefined ? undefined : (
        <HStack alignItems="flex-start" bg="bg.muted" borderRadius="md" flexWrap="wrap" gap={6} mb={2} p={3}>
          {[
            {
              label: translate("callbacks.columns.alertName"),
              value:
                deadline.alert_name === null && completionRule === undefined ? undefined : (
                  <>
                    {deadline.alert_name}
                    {completionRule === undefined ? undefined : (
                      <Text color="fg.muted" fontSize="xs">
                        {completionRule}
                      </Text>
                    )}
                  </>
                ),
            },
            {
              label: translate("callbacks.columns.deadlineTime"),
              value: (
                <HStack gap={2}>
                  <Time datetime={deadline.deadline_time} />
                  <Badge colorPalette={deadline.missed ? "red" : "blue"} size="sm" variant="solid">
                    {deadline.missed ? <FiAlertTriangle /> : <FiClock />}
                    {translate(deadline.missed ? "deadlineStatus.missed" : "deadlineStatus.upcoming")}
                  </Badge>
                </HStack>
              ),
            },
            {
              label: translate("callbacks.columns.missedBy"),
              value: getMissedBy({ deadline, renderDuration, runEndDate: dagRun?.end_date, translate }),
            },
            {
              label: translate("callbacks.columns.type"),
              value: translateCallbackType(translate, deadline.callback_type),
            },
            {
              label: translate("common:state"),
              value: <CallbackStateBadge state={deadline.callback_state} />,
            },
            { label: translate("callbacks.columns.callback"), value: deadline.callback_path },
          ]
            .filter(({ value }) => value !== null && value !== undefined)
            .map(({ label, value }) => (
              <Box key={label}>
                <Box
                  color="fg.muted"
                  fontSize="xs"
                  fontWeight="medium"
                  lineHeight="1"
                  textTransform="uppercase"
                >
                  {label}
                </Box>
                <Box fontSize="sm" mt={1}>
                  {value}
                </Box>
              </Box>
            ))}
        </HStack>
      )}
      <TaskLogHeader {...logHeaderProps} />
      <TaskLogContent {...logContentProps} />
      <Modal
        bodyProps={{ display: "flex", flexDirection: "column" }}
        headerProps={{
          children: (
            <Box display="flex" flexDirection="column" width="100%">
              <Heading mb={2} size="xl">
                {callbackId}
              </Heading>
              <TaskLogHeader {...logHeaderProps} isFullscreen />
            </Box>
          ),
          width: "100%",
        }}
        onOpenChange={() => setFullscreen(false)}
        open={fullscreen}
        scrollBehavior="inside"
        size="full"
      >
        <TaskLogContent {...logContentProps} />
      </Modal>
    </Box>
  );
};
