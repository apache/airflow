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
import { Box } from "@chakra-ui/react";
import { useParams } from "react-router-dom";
import { useSearchParams } from "react-router-dom";

import {
  useTaskInstanceServiceGetHitlDetailTryDetail,
  useTaskInstanceServiceGetMappedTaskInstance,
} from "openapi/queries";
import { TaskTrySelect } from "src/components/TaskTrySelect";
import { ProgressBar } from "src/components/ui";
import { SearchParamsKeys } from "src/constants/searchParams";
import { isStatePending, useAutoRefresh } from "src/utils";

import { HITLResponseForm } from "../HITLTaskInstances/HITLResponseForm";

export const HITLResponse = () => {
  const { dagId, mapIndex, runId, taskId } = useParams();

  const refetchInterval = useAutoRefresh({ dagId });
  const [searchParams, setSearchParams] = useSearchParams();
  const tryNumberParam = searchParams.get(SearchParamsKeys.TRY_NUMBER);

  const parsedMapIndex = Number(mapIndex ?? -1);

  const { data: taskInstance } = useTaskInstanceServiceGetMappedTaskInstance(
    {
      dagId: dagId ?? "",
      dagRunId: runId ?? "",
      mapIndex: parsedMapIndex,
      taskId: taskId ?? "",
    },
    undefined,
    {
      enabled: !isNaN(parsedMapIndex),
      refetchInterval: (query) => (isStatePending(query.state.data?.state) ? refetchInterval : false),
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

  const { data: hitlDetail } = useTaskInstanceServiceGetHitlDetailTryDetail(
    {
      dagId: dagId ?? "",
      dagRunId: runId ?? "",
      mapIndex: parsedMapIndex,
      taskId: taskId ?? "",
      tryNumber: tryNumber ?? 1,
    },
    undefined,
  );

  if (!taskInstance || !hitlDetail) {
    return (
      <Box flexGrow={1}>
        <ProgressBar />
      </Box>
    );
  }

  return (
    <Box px={4}>
      {taskInstance.try_number <= 1 ? undefined : (
        <TaskTrySelect
          onSelectTryNumber={onSelectTryNumber}
          selectedTryNumber={tryNumber}
          taskInstance={taskInstance}
        />
      )}
      <HITLResponseForm hitlDetail={hitlDetail} />
    </Box>
  );
};
