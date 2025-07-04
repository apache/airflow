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
import { useParams, useSearchParams } from "react-router-dom";

import type { LightGridTaskInstanceSummary } from "openapi/requests/types.gen";

import { GridTI } from "./GridTI";
import type { GridTask } from "./utils";

type Props = {
  readonly depth?: number;
  readonly nodes: Array<GridTask>;
  readonly runId: string;
  readonly taskInstances: Array<LightGridTaskInstanceSummary>;
};

export const TaskInstancesColumn = ({ nodes, runId, taskInstances }: Props) => {
  const { dagId = "" } = useParams();
  const [searchParams] = useSearchParams();
  const search = searchParams.toString();

  return nodes.map((node) => {
    // todo: how does this work with mapped? same task id for multiple tis
    const taskInstance = taskInstances.find((ti) => ti.task_id === node.id);

    if (!taskInstance) {
      return <Box height="20px" key={`${node.id}-${runId}`} width="18px" />;
    }

    return (
      <GridTI
        dagId={dagId}
        isGroup={node.isGroup}
        isMapped={node.is_mapped}
        key={node.id}
        label={node.label}
        runId={runId}
        search={search}
        state={taskInstance.state}
        taskId={node.id}
      />
    );
  });
};
