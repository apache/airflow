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
import { useParams, useSearchParams } from "react-router-dom";

import { useTaskInstanceServiceGetMappedTaskInstance } from "openapi/queries";
import { Dialog } from "src/components/ui";
import { useConfig } from "src/queries/useConfig";
import { useLogs } from "src/queries/useLogs";

import { TaskLogContent } from "./TaskLogContent";
import { TaskLogHeader } from "./TaskLogHeader";

export const Logs = () => {
  const { dagId = "", runId = "", taskId = "" } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();

  const mapIndexParam = searchParams.get("map_index");
  const tryNumberParam = searchParams.get("try_number");
  const mapIndex = parseInt(mapIndexParam ?? "-1", 10);

  const {
    data: taskInstance,
    error,
    isLoading,
  } = useTaskInstanceServiceGetMappedTaskInstance({
    dagId,
    dagRunId: runId,
    mapIndex,
    taskId,
  });

  const onSelectTryNumber = (newTryNumber: number) => {
    if (newTryNumber === taskInstance?.try_number) {
      searchParams.delete("try_number");
    } else {
      searchParams.set("try_number", newTryNumber.toString());
    }
    setSearchParams(searchParams);
  };

  const tryNumber = tryNumberParam === null ? taskInstance?.try_number : parseInt(tryNumberParam, 10);

  const defaultWrap = Boolean(useConfig("default_wrap"));

  const [wrap, setWrap] = useState(defaultWrap);
  const [fullscreen, setFullscreen] = useState(false);

  const toggleWrap = () => setWrap(!wrap);
  const toggleFullscreen = () => setFullscreen(!fullscreen);

  const onOpenChange = () => {
    setFullscreen(false);
  };

  const {
    data,
    error: logError,
    isLoading: isLoadingLogs,
  } = useLogs({
    dagId,
    mapIndex,
    runId,
    taskId,
    tryNumber: tryNumber ?? 1,
  });

  return (
    <Box p={2}>
      <TaskLogHeader
        onSelectTryNumber={onSelectTryNumber}
        taskInstance={taskInstance}
        toggleFullscreen={toggleFullscreen}
        toggleWrap={toggleWrap}
        tryNumber={tryNumber}
        wrap={wrap}
      />
      <TaskLogContent
        error={error}
        isLoading={isLoading || isLoadingLogs}
        logError={logError}
        parsedLogs={data.parsedLogs}
        wrap={wrap}
      />
      <Dialog.Root onOpenChange={onOpenChange} open={fullscreen} scrollBehavior="inside" size="full">
        <Dialog.Content backdrop>
          <Dialog.Header>
            <Heading size="xl">{taskId}</Heading>
            <TaskLogHeader
              isFullscreen
              onSelectTryNumber={onSelectTryNumber}
              taskInstance={taskInstance}
              toggleFullscreen={toggleFullscreen}
              toggleWrap={toggleWrap}
              tryNumber={tryNumber}
              wrap={wrap}
            />
          </Dialog.Header>

          <Dialog.CloseTrigger />

          <Dialog.Body>
            <TaskLogContent
              error={error}
              isLoading={isLoading || isLoadingLogs}
              logError={logError}
              parsedLogs={data.parsedLogs}
              wrap={wrap}
            />
          </Dialog.Body>
        </Dialog.Content>
      </Dialog.Root>
    </Box>
  );
};
