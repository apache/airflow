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
import type {
  AssetResponse,
  BaseEdgeResponse,
  BaseGraphResponse,
  BaseNodeResponse,
} from "openapi/requests/types.gen";

export const taskNodeSeparator = "SEPARATOR";

export const getTaskLevelDependencies = (
  baseGraphData: { edges: Array<BaseEdgeResponse>; nodes: Array<BaseNodeResponse> },
  asset: AssetResponse | undefined,
): BaseGraphResponse => {
  if (!asset) {
    return {
      edges: baseGraphData.edges,
      nodes: baseGraphData.nodes,
    };
  }

  const assetId = `asset:${asset.id}`;
  const nodes: Array<BaseNodeResponse> = [...baseGraphData.nodes];
  const edges: Array<BaseEdgeResponse> = [];
  const nodeIds = new Set(nodes.map((node) => node.id));

  const addTaskNode = (dagId: string, taskId: string) => {
    const compositeId = `task:${dagId}${taskNodeSeparator}${taskId}`;

    nodes.push({ id: compositeId, label: taskId, type: "task" });
    nodeIds.add(compositeId);

    return compositeId;
  };

  asset.producing_tasks.forEach(({ dag_id: dagId, task_id: taskId }) => {
    edges.push({ source_id: addTaskNode(dagId, taskId), target_id: assetId });
  });

  asset.consuming_tasks.forEach(({ dag_id: dagId, task_id: taskId }) => {
    edges.push({ source_id: assetId, target_id: addTaskNode(dagId, taskId) });
  });

  edges.push(...baseGraphData.edges);

  return { edges, nodes };
};
