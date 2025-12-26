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

import { useTaskInstanceServiceGetHitlDetail } from "openapi/queries";
import { ProgressBar } from "src/components/ui";

import { HITLResponseForm } from "../HITLTaskInstances/HITLResponseForm";

export const HITLResponse = () => {
  const { dagId, mapIndex, runId, taskId } = useParams();

  const { data: hitlDetail } = useTaskInstanceServiceGetHitlDetail(
    {
      dagId: dagId ?? "",
      dagRunId: runId ?? "",
      mapIndex: Number(mapIndex ?? -1),
      taskId: taskId ?? "",
    },
    undefined,
  );

  if (!hitlDetail?.task_instance) {
    return (
      <Box flexGrow={1}>
        <ProgressBar />
      </Box>
    );
  }

  return (
    <Box px={4}>
      <HITLResponseForm hitlDetail={hitlDetail} />
    </Box>
  );
};
