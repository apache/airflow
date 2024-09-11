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
import { Box, useTheme } from "@chakra-ui/react";
import { Handle, NodeProps, Position } from "reactflow";
import { TbLogicAnd, TbLogicOr } from "react-icons/tb";

import type { DepNode, DagRun, Task, TaskInstance } from "src/types";
import type { DatasetEvent } from "src/types/api-generated";

import Tooltip from "src/components/Tooltip";
import { useContainerRef } from "src/context/containerRef";
import { hoverDelay } from "src/utils";
import DatasetNode from "./DatasetNode";
import DagNode from "./DagNode";

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
  datasetEvent?: DatasetEvent;
}

const Node = (props: NodeProps<CustomNodeProps>) => {
  const { colors } = useTheme();
  const { data } = props;
  const containerRef = useContainerRef();

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

  if (data.class === "or-gate" || data.class === "and-gate") {
    return (
      <Box
        height={`${data.height}px`}
        width={`${data.width}px`}
        borderRadius={4}
        borderWidth={1}
      >
        <Tooltip
          label={data.class === "or-gate" ? "Or" : "And"}
          portalProps={{ containerRef }}
          hasArrow
          openDelay={hoverDelay}
        >
          <Box>
            {data.class === "or-gate" ? (
              <TbLogicOr size="30px" stroke={colors.gray[600]} />
            ) : (
              <TbLogicAnd size="30px" stroke={colors.gray[600]} />
            )}
          </Box>
        </Tooltip>
      </Box>
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
