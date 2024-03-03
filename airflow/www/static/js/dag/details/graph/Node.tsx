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
import { Box } from "@chakra-ui/react";
import { Handle, NodeProps, Position } from "reactflow";

import type { DepNode, DagRun, Task, TaskInstance } from "src/types";

import DagNode from "./DagNode";
import DatasetNode from "./DatasetNode";

export interface CustomNodeProps {
  label: string;
  height?: number;
  width?: number;
  isJoinNode?: boolean;
  instance?: TaskInstance;
  task?: Task | null;
  isSelected: boolean;
  latestDagRunId: DagRun["runId"];
  childCount?: number;
  onToggleCollapse: () => void;
  isOpen?: boolean;
  isActive?: boolean;
  setupTeardownType?: "setup" | "teardown";
  labelStyle?: string;
  style?: string;
  isZoomedOut: boolean;
  class: DepNode["value"]["class"];
}

const Node = (props: NodeProps<CustomNodeProps>) => {
  const { data } = props;

  if (data.isJoinNode) {
    return (
      <Box
        height={`${data.height}px`}
        width={`${data.width}px`}
        borderRadius={data.width}
        bg="gray.400"
      />
    );
  }

  if (data.class === "dataset") return <DatasetNode {...props} />;

  return <DagNode {...props} />;
};

const NodeWrapper = (props: NodeProps<CustomNodeProps>) => (
  <>
    <Handle
      type="target"
      position={Position.Top}
      style={{ visibility: "hidden" }}
    />
    <Node {...props} />
    <Handle
      type="source"
      position={Position.Bottom}
      style={{ visibility: "hidden" }}
    />
  </>
);

export default NodeWrapper;
