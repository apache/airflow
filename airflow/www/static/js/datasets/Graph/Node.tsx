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
import { Flex, Text, useTheme } from "@chakra-ui/react";
import { Group } from "@visx/group";
import { MdPlayArrow, MdSensors } from "react-icons/md";
import { HiDatabase } from "react-icons/hi";

import type { ElkShape } from "elkjs";
import type { DepNode } from "src/types";

import DagNode from "./DagNode";

export interface NodeType extends ElkShape {
  value: DepNode["value"];
  children?: NodeType[];
}

interface Props {
  node: NodeType;
  onSelect: (datasetId: string) => void;
  isSelected: boolean;
  isHighlighted: boolean;
}

const Node = ({
  node: { height, width, x, y, value },
  onSelect,
  isSelected,
  isHighlighted,
}: Props) => {
  const { colors } = useTheme();
  return (
    <Group top={y} left={x} height={height} width={width}>
      <foreignObject width={width} height={height}>
        {value.class === "dag" && (
          <DagNode dagId={value.label} isHighlighted={isHighlighted} />
        )}
        {value.class !== "dag" && (
          <Flex
            borderWidth={isSelected ? 4 : 2}
            borderColor={
              isHighlighted || isSelected ? colors.blue[400] : undefined
            }
            borderRadius={5}
            p={2}
            height="100%"
            width="100%"
            fontWeight={isSelected ? "bold" : "normal"}
            onClick={(e) => {
              e.preventDefault();
              e.stopPropagation();
              if (value.class === "dataset") onSelect(value.label);
            }}
            cursor="pointer"
            fontSize={16}
            justifyContent="space-between"
            alignItems="center"
          >
            {value.class === "dataset" && <HiDatabase size="16px" />}
            {value.class === "sensor" && <MdSensors size="16px" />}
            {value.class === "trigger" && <MdPlayArrow size="16px" />}
            <Text>{value.label}</Text>
          </Flex>
        )}
      </foreignObject>
    </Group>
  );
};

export default Node;
