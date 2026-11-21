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
import { forwardRef } from "react";

import { useParams, useSearchParams, Link as RouterLink } from "react-router-dom";

import { AltClickExtraLinks } from "src/components/AltClickExtraLinks";
import { TaskName, type TaskNameProps } from "src/components/TaskName";

import type { ExtraLink } from "src/hooks/useTaskInstanceExtraLinks";
import { taskNodeSeparator } from "src/utils/assetGraph";
import { setExecutionCoordinates, stripExecutionParams } from "src/utils/executionParams";

const NO_EXTRA_LINKS: ReadonlyArray<ExtraLink> = [];

export const parseTaskNodeId = (compositeId: string) => {
  const match = new RegExp(`^task:(?<dagId>.*?)${taskNodeSeparator}(?<taskId>.+)$`, "u").exec(compositeId);

  return match ? { dagId: match[1], taskId: match[2] } : { dagId: undefined, taskId: undefined };
};

type Props = {
  readonly dagId?: string;
  readonly extraLinks?: ReadonlyArray<ExtraLink>;
  readonly hasTaskInstance?: boolean;
  readonly id: string;
  // A task inside a loop has one row per pass, so a link with no coordinate is ambiguous and
  // the API rejects it. The graph draws the task once, so it points at the newest pass.
  readonly latestRegionId?: string | null;
  readonly latestRegionIndex?: number | null;
} & TaskNameProps;

export const TaskLink = forwardRef<HTMLAnchorElement, Props>(
  (
    {
      extraLinks = NO_EXTRA_LINKS,
      hasTaskInstance = true,
      id,
      isGroup,
      isMapped,
      latestRegionId,
      latestRegionIndex,
      ...rest
    },
    ref,
  ) => {
    const { dagId: urlDagId = "", groupId, runId, taskId: urlTaskId } = useParams();
    const [searchParams] = useSearchParams();

    const { dagId: extractedDagId, taskId: extractedTaskId } = parseTaskNodeId(id);
    const dagId = extractedDagId ?? urlDagId;
    const taskId = extractedTaskId ?? id;

    const includeRun = runId !== undefined && hasTaskInstance;
    const basePath = `/dags/${dagId}${includeRun ? `/runs/${runId}` : ""}`;
    const taskPath = isGroup
      ? includeRun && groupId === taskId
        ? ""
        : `/tasks/group/${taskId}`
      : includeRun && urlTaskId === taskId
        ? ""
        : `/tasks/${taskId}${isMapped && urlTaskId !== taskId && includeRun ? "/mapped" : ""}`;
    const taskTarget = {
      pathname: basePath + taskPath,
      search: setExecutionCoordinates(
        stripExecutionParams(searchParams),
        latestRegionId,
        latestRegionIndex,
      ).toString(),
    };

    return (
      <AltClickExtraLinks links={extraLinks} navigateTo={taskTarget}>
        <RouterLink ref={ref} to={taskTarget}>
          <TaskName isGroup={isGroup} isMapped={isMapped} {...rest} />
        </RouterLink>
      </AltClickExtraLinks>
    );
  },
);
