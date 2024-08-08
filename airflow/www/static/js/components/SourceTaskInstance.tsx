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
import { Box, Link, Tooltip, Flex } from "@chakra-ui/react";
import { FiLink } from "react-icons/fi";

import { useTaskInstance } from "src/api";
import type { DatasetEvent } from "src/types/api-generated";
import { useContainerRef } from "src/context/containerRef";
import { SimpleStatus } from "src/dag/StatusBox";
import InstanceTooltip from "src/components/InstanceTooltip";
import type { TaskInstance } from "src/types";
import { getMetaValue } from "src/utils";

type SourceTIProps = {
  datasetEvent: DatasetEvent;
  showLink?: boolean;
};

const gridUrl = getMetaValue("grid_url");
const dagId = getMetaValue("dag_id") || "__DAG_ID__";

const SourceTaskInstance = ({
  datasetEvent,
  showLink = true,
}: SourceTIProps) => {
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

  let url = `${gridUrl?.replace(
    dagId,
    sourceDagId || ""
  )}?dag_run_id=${encodeURIComponent(
    sourceRunId || ""
  )}&task_id=${encodeURIComponent(sourceTaskId || "")}`;

  if (
    sourceMapIndex !== null &&
    sourceMapIndex !== undefined &&
    sourceMapIndex > -1
  ) {
    url = `${url}&map_index=${sourceMapIndex}`;
  }

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
          <Flex width={showLink ? "30px" : "16px"}>
            <SimpleStatus state={taskInstance.state} mx={1} />
            {showLink && (
              <Link color="blue.600" href={url}>
                <FiLink size="12px" />
              </Link>
            )}
          </Flex>
        </Tooltip>
      )}
    </Box>
  );
};

export default SourceTaskInstance;
