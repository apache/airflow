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
import { Box, Text, Flex, useTheme } from "@chakra-ui/react";
import { Handle, NodeProps, Position } from "reactflow";
import { MdPlayArrow, MdSensors } from "react-icons/md";
import { HiDatabase } from "react-icons/hi";
import { PiRectangleDashed } from "react-icons/pi";

import DagNode from "./DagNode";

export interface CustomNodeProps {
  label: string;
  type?: string;
  height?: number;
  width?: number;
  isSelected?: boolean;
  isHighlighted?: boolean;
  onSelect: () => void;
  isOpen?: boolean;
  isActive?: boolean;
}

const BaseNode = ({
  data: { label, type, isSelected, isHighlighted, onSelect },
}: NodeProps<CustomNodeProps>) => {
  const { colors } = useTheme();

  return (
    <Box bg="white">
      {type === "dag" && (
        <DagNode
          dagId={label}
          isHighlighted={isHighlighted}
          isSelected={isSelected}
          onSelect={onSelect}
        />
      )}
      {type !== "dag" && (
        <Flex
          borderWidth={isSelected ? 4 : 2}
          borderColor={isSelected ? colors.blue[400] : undefined}
          borderRadius={5}
          p={2}
          fontWeight={isSelected ? "bold" : "normal"}
          onClick={(e) => {
            e.preventDefault();
            e.stopPropagation();
            onSelect();
          }}
          cursor="pointer"
          fontSize={16}
          justifyContent="space-between"
          alignItems="center"
        >
          {type === "dataset" && <HiDatabase size="16px" />}
          {type === "sensor" && <MdSensors size="16px" />}
          {type === "trigger" && <MdPlayArrow size="16px" />}
          {type === "dataset-alias" && <PiRectangleDashed size="16px" />}
          <Text ml={2}>{label}</Text>
        </Flex>
      )}
    </Box>
  );
};

const Node = (props: NodeProps<CustomNodeProps>) => (
  <>
    <Handle
      type="target"
      position={Position.Top}
      style={{ visibility: "hidden" }}
    />
    <BaseNode {...props} />
    <Handle
      type="source"
      position={Position.Bottom}
      style={{ visibility: "hidden" }}
    />
  </>
);

export default Node;
