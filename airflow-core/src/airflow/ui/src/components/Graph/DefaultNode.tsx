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
import { Flex, Text } from "@chakra-ui/react";
import type { NodeProps, Node as NodeType } from "@xyflow/react";

import { NodeWrapper } from "./NodeWrapper";
import type { CustomNodeProps } from "./reactflowUtils";

export const DefaultNode = ({ data: { height, label, width } }: NodeProps<NodeType<CustomNodeProps>>) => (
  <NodeWrapper>
    <Flex
      bg="bg"
      borderRadius={5}
      borderWidth={2}
      flexDirection="column"
      height={`${height}px`}
      p={2}
      width={`${width}px`}
    >
      <Text>{label}</Text>
    </Flex>
  </NodeWrapper>
);
