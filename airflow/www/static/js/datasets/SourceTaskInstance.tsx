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

import React from "react";
import { Box, Tooltip } from "@chakra-ui/react";

import { useTaskInstance } from "src/api";
import type { DatasetEvent } from "src/types/api-generated";
import { useContainerRef } from "src/context/containerRef";
import { SimpleStatus } from "src/dag/StatusBox";
import InstanceTooltip from "src/components/InstanceTooltip";
import type { TaskInstance } from "src/types";

type SourceTIProps = {
  datasetEvent: DatasetEvent;
};

const SourceTaskInstance = ({ datasetEvent }: SourceTIProps) => {
  const containerRef = useContainerRef();
  const { sourceDagId, sourceRunId, sourceTaskId, sourceMapIndex } =
    datasetEvent;

  const { data: taskInstance } = useTaskInstance({
    dagId: sourceDagId || "",
    dagRunId: sourceRunId || "",
    taskId: sourceTaskId || "",
    mapIndex: sourceMapIndex || undefined,
    options: {
      enabled: !!(sourceDagId && sourceRunId && sourceTaskId),
      refetchInterval: false,
    },
  });

  return (
    <Box>
      {!!taskInstance && (
        <Tooltip
          label={
            <InstanceTooltip
              instance={{ ...taskInstance, runId: sourceRunId } as TaskInstance}
              dagId={sourceDagId || undefined}
            />
          }
          portalProps={{ containerRef }}
          hasArrow
          placement="top"
        >
          <Box width="12px">
            <SimpleStatus state={taskInstance.state} mx={1} />
          </Box>
        </Tooltip>
      )}
    </Box>
  );
};

export default SourceTaskInstance;
