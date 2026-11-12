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
import { Box, Flex, Heading, HStack, LinkOverlay } from "@chakra-ui/react";
import type { NodeProps, Node as NodeType } from "@xyflow/react";
import { FiEye } from "react-icons/fi";
import { Link as RouterLink } from "react-router-dom";

import { TriggerStatusBadge } from "../TriggerStatusBadge";
import { NodeWrapper } from "./NodeWrapper";
import type { CustomNodeProps } from "./reactflowUtils";

export const AssetWatcherNode = ({
  data: { height, label, triggerId, triggerStatus, width },
}: NodeProps<NodeType<CustomNodeProps, "asset-watcher">>) => (
  <NodeWrapper>
    <Flex
      bg="bg"
      borderColor="border"
      borderRadius={5}
      borderWidth={2}
      cursor="default"
      flexDirection="column"
      gap={1}
      height={`${height}px`}
      px={3}
      py={1}
      width={`${width}px`}
    >
      <HStack>
        <Heading ml={-2} size="sm">
          <FiEye />
        </Heading>
        <LinkOverlay asChild>
          <RouterLink to={`/triggers/${triggerId}`}>{label}</RouterLink>
        </LinkOverlay>
      </HStack>
      {triggerStatus === undefined || triggerStatus === null ? undefined : (
        // Above the link overlay, so its tooltip explaining the status still opens.
        <Box position="relative" width="fit-content" zIndex={1}>
          <TriggerStatusBadge size="sm" status={triggerStatus} />
        </Box>
      )}
    </Flex>
  </NodeWrapper>
);
