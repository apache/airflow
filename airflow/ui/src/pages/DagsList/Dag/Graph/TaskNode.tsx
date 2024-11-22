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
import { Box, Button, Flex, Text } from "@chakra-ui/react";
import type { NodeProps, Node as NodeType } from "@xyflow/react";

import { useOpenGroups } from "src/context/openGroups";
import { pluralize } from "src/utils";

import { NodeWrapper } from "./NodeWrapper";
import { TaskName } from "./TaskName";
import type { CustomNodeProps } from "./reactflowUtils";

export const TaskNode = ({
  data: {
    childCount,
    height,
    isGroup,
    isMapped,
    isOpen,
    label,
    setupTeardownType,
    width,
  },
  id,
}: NodeProps<NodeType<CustomNodeProps, "task">>) => {
  const { toggleGroupId } = useOpenGroups();
  const onClick = () => {
    if (isGroup) {
      toggleGroupId(id);
    }
  };

  return (
    <NodeWrapper>
      <Flex alignItems="center" flexDirection="column">
        <Flex
          bg="bg"
          borderColor="fg"
          borderRadius={5}
          borderWidth={1}
          height={`${height}px`}
          justifyContent="space-between"
          px={3}
          py={2}
          width={`${width}px`}
        >
          <Box>
            {/* TODO: replace 'Operator' */}
            <Text fontSize="xs" fontWeight="lighter" textTransform="uppercase">
              {isGroup ? "Task Group" : "Operator"}
            </Text>
            <TaskName
              id={id}
              isGroup={isGroup}
              isMapped={isMapped}
              isOpen={isOpen}
              label={label}
              setupTeardownType={setupTeardownType}
            />
          </Box>
          <Box>
            {isGroup ? (
              <Button
                colorPalette="blue"
                onClick={onClick}
                p={0}
                variant="plain"
              >
                {isOpen ? "- " : "+ "}
                {pluralize("task", childCount, undefined, false)}
              </Button>
            ) : undefined}
          </Box>
        </Flex>
        {Boolean(isMapped) || Boolean(isGroup && !isOpen) ? (
          <>
            <Box
              bg="bg.subtle"
              borderBottomLeftRadius={5}
              borderBottomRightRadius={5}
              borderBottomWidth={1}
              borderColor="fg"
              borderLeftWidth={1}
              borderRightWidth={1}
              height={1}
              width={`${(width ?? 0) - 10}px`}
            />
            <Box
              bg="bg.subtle"
              borderBottomLeftRadius={5}
              borderBottomRightRadius={5}
              borderBottomWidth={1}
              borderColor="fg"
              borderLeftWidth={1}
              borderRightWidth={1}
              height={1}
              width={`${(width ?? 0) - 20}px`}
            />
          </>
        ) : undefined}
      </Flex>
    </NodeWrapper>
  );
};
