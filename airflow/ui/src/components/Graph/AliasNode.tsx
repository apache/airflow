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
import { Flex, Heading, HStack } from "@chakra-ui/react";
import type { NodeProps, Node as NodeType } from "@xyflow/react";
import { PiRectangleDashed } from "react-icons/pi";

import { NodeWrapper } from "./NodeWrapper";
import type { CustomNodeProps } from "./reactflowUtils";

export const AliasNode = ({
  data: { height, isSelected, label, width },
}: NodeProps<NodeType<CustomNodeProps, "asset">>) => (
  <NodeWrapper>
    <Flex
      bg="bg"
      borderColor={isSelected ? "border.inverted" : "border"}
      borderRadius={5}
      borderWidth={isSelected ? 6 : 2}
      cursor="default"
      flexDirection="column"
      height={`${height}px`}
      px={3}
      py={isSelected ? 0 : 1}
      width={`${width}px`}
    >
      <HStack>
        <Heading size="sm">
          <PiRectangleDashed />
        </Heading>
        {label}
      </HStack>
    </Flex>
  </NodeWrapper>
);
