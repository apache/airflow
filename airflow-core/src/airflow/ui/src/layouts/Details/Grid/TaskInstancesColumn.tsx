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
import type { VirtualItem } from "@tanstack/react-virtual";
import { memo, useMemo } from "react";
import { useParams } from "react-router-dom";

import type { LightGridTaskInstanceSummary } from "openapi/requests/types.gen";

import { GridTI } from "./GridTI";
import type { GridTask } from "./utils";

type Props = {
  readonly depth?: number;
  readonly nodes: Array<GridTask>;
  readonly onCellClick?: () => void;
  readonly runId: string;
  readonly taskInstances: Array<LightGridTaskInstanceSummary>;
  readonly virtualItems?: Array<VirtualItem>;
};

const ROW_HEIGHT = 20;

const TaskInstancesColumnInner = ({ nodes, onCellClick, runId, taskInstances, virtualItems }: Props) => {
  const { dagId = "" } = useParams();

  const itemsToRender =
    virtualItems ?? nodes.map((_, index) => ({ index, size: ROW_HEIGHT, start: index * ROW_HEIGHT }));

  const taskInstanceMap = useMemo(() => {
    const map = new Map<string, LightGridTaskInstanceSummary>();

    for (const ti of taskInstances) {
      map.set(ti.task_id, ti);
    }

    return map;
  }, [taskInstances]);

  return itemsToRender.map((virtualItem) => {
    const node = nodes[virtualItem.index];

    if (!node) {
      return undefined;
    }

    const taskInstance = taskInstanceMap.get(node.id);

    if (!taskInstance) {
      return (
        <Box
          height={`${ROW_HEIGHT}px`}
          key={`${node.id}-${runId}`}
          left={0}
          position="absolute"
          top={0}
          transform={`translateY(${virtualItem.start}px)`}
          width="18px"
        />
      );
    }

    return (
      <Box
        key={node.id}
        left={0}
        position="absolute"
        top={0}
        transform={`translateY(${virtualItem.start}px)`}
      >
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

export const TaskInstancesColumn = memo(TaskInstancesColumnInner);
