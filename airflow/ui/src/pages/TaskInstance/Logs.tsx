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
import { Box, Code, HStack, Skeleton, VStack } from "@chakra-ui/react";
import { useState } from "react";
import { useParams, useSearchParams } from "react-router-dom";

import { useTaskInstanceServiceGetMappedTaskInstance } from "openapi/queries";
import { ErrorAlert } from "src/components/ErrorAlert";
import { TaskTrySelect } from "src/components/TaskTrySelect";
import { Button, ProgressBar } from "src/components/ui";
import { useConfig } from "src/queries/useConfig";
import { useLogs } from "src/queries/useLogs";

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

  const tryNumber =
    tryNumberParam === null
      ? taskInstance?.try_number
      : parseInt(tryNumberParam, 10);

  const defaultWrap = Boolean(useConfig("default_wrap"));

  const [wrap, setWrap] = useState(defaultWrap);

  const toggleWrap = () => setWrap(!wrap);

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
      <HStack justifyContent="space-between" mb={2}>
        {taskInstance === undefined ||
        tryNumber === undefined ||
        taskInstance.try_number <= 1 ? (
          <div />
        ) : (
          <TaskTrySelect
            onSelectTryNumber={onSelectTryNumber}
            selectedTryNumber={tryNumber}
            taskInstance={taskInstance}
          />
        )}
        <Button
          aria-label={wrap ? "Unwrap" : "Wrap"}
          bg="bg.panel"
          onClick={toggleWrap}
          variant="outline"
        >
          {wrap ? "Unwrap" : "Wrap"}
        </Button>
      </HStack>
      <ErrorAlert error={error ?? logError} />
      <Skeleton />
      <ProgressBar
        size="xs"
        visibility={isLoading || isLoadingLogs ? "visible" : "hidden"}
      />
      <Code overflow="auto" py={3} textWrap={wrap ? "pre" : "nowrap"}>
        <VStack alignItems="flex-start">{data.parsedLogs}</VStack>
      </Code>
    </Box>
  );
};
