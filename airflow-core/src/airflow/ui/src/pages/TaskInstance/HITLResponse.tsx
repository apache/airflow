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
import { useEffect } from "react";
import { useParams } from "react-router-dom";

import { useHumanInTheLoopServiceGetMappedTiHitlDetail } from "openapi/queries";
import { ErrorAlert } from "src/components/ErrorAlert";
import { ProgressBar } from "src/components/ui";

import { HITLResponseForm } from "../HITLTaskInstances/HITLResponseForm";
import { useHITLResponseState } from "../HITLTaskInstances/useHITLResponseState";

export const HITLResponse = () => {
  const { dagId = "", mapIndex = "-1", runId = "", taskId = "" } = useParams();

  const {
    data: hitlDetail,
    error,
    isLoading,
  } = useHumanInTheLoopServiceGetMappedTiHitlDetail(
    {
      dagId,
      dagRunId: runId,
      mapIndex: parseInt(mapIndex, 10),
      taskId,
    },
    undefined,
    {
      enabled: Boolean(dagId && runId && taskId),
    },
  );

  const { onOpen } = useHITLResponseState();

  // Set the task instance in the state when HITL detail is loaded
  useEffect(() => {
    if (hitlDetail?.task_instance) {
      onOpen(hitlDetail.task_instance);
    }
  }, [hitlDetail?.task_instance, onOpen]);

  if (isLoading) {
    return (
      <Box flexGrow={1}>
        <ProgressBar />
      </Box>
    );
  }

  if (Boolean(error)) {
    return <ErrorAlert error={error} />;
  }

  return (
    <Box px={4}>
      <HITLResponseForm />
    </Box>
  );
};
