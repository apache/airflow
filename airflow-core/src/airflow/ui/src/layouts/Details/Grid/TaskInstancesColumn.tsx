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

import type { LightGridTaskInstanceSummary } from "openapi/requests/types.gen";
import { DagVersionIndicator } from "src/components/ui/VersionIndicator";
import { VersionIndicatorDisplayOptions } from "src/constants/showVersionIndicatorOptions";

import { GridTI } from "./GridTI";
import type { GridTask } from "./utils";

type Props = {
  readonly depth?: number;
  readonly hasMixedVersions?: boolean;
  readonly nodes: Array<GridTask>;
  readonly onCellClick?: () => void;
  readonly runId: string;
  readonly showVersionIndicatorMode?: string;
  readonly taskInstances: Array<LightGridTaskInstanceSummary>;
};

export const TaskInstancesColumn = ({
  hasMixedVersions,
  nodes,
  onCellClick,
  runId,
  showVersionIndicatorMode,
  taskInstances,
}: Props) => {
  const { dagId = "" } = useParams();

  const taskInstanceMap = new Map(taskInstances.map((ti) => [ti.task_id, ti]));

  return nodes.map((node, idx) => {
    // todo: how does this work with mapped? same task id for multiple tis
    const taskInstance = taskInstanceMap.get(node.id);

    if (!taskInstance) {
      return <Box height="20px" key={`${node.id}-${runId}`} width="18px" />;
    }

    const hasVersionChangeFlag =
      hasMixedVersions &&
      (showVersionIndicatorMode === VersionIndicatorDisplayOptions.DAG ||
        showVersionIndicatorMode === VersionIndicatorDisplayOptions.ALL) &&
      idx > 0 &&
      (() => {
        const prevNode = nodes[idx - 1];

        if (!prevNode) {
          return false;
        }
        const prevTaskInstance = taskInstanceMap.get(prevNode.id);

        return prevTaskInstance && prevTaskInstance.dag_version_number !== taskInstance.dag_version_number;
      })();

    return (
      <Box key={node.id} position="relative">
        {hasVersionChangeFlag ? (
          <DagVersionIndicator
            dagVersionNumber={taskInstance.dag_version_number ?? null}
            orientation="horizontal"
          />
        ) : null}
        <GridTI
          dagId={dagId}
          instance={taskInstance}
          isGroup={node.isGroup}
          isMapped={node.is_mapped}
          label={node.label}
          onClick={onCellClick}
          runId={runId}
          taskId={node.id}
        />
      </Box>
    );
  });
};
