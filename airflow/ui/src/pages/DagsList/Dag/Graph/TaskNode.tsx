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
import { Button, Flex } from "@chakra-ui/react";
import type { NodeProps, Node as NodeType } from "@xyflow/react";

import { NodeWrapper } from "./NodeWrapper";
import { TaskName } from "./TaskName";

export type CustomNodeProps = {
  childCount?: number;
  height?: number;
  isActive?: boolean;
  isOpen?: boolean;
  label: string;
  onToggleGroups: (groupIds: Array<string>) => void;
  openGroupIds: Array<string>;
  setupTeardownType?: "setup" | "teardown";
  style?: string;
  width?: number;
};

export const TaskNode = ({
  data,
  id,
}: NodeProps<NodeType<CustomNodeProps, "task">>) => {
  const onClick = () => {
    if (data.childCount !== undefined) {
      data.onToggleGroups(
        data.isOpen
          ? data.openGroupIds.filter((groupId) => groupId !== id)
          : [...data.openGroupIds, id],
      );
    }
  };

  return (
    <NodeWrapper>
      <Flex
        bg="bg"
        borderColor="fg"
        borderRadius={5}
        borderWidth={1}
        height={`${data.height}px`}
        px={3}
        py={2}
        width={`${data.width}px`}
      >
        <Button onClick={onClick} variant="plain">
          <TaskName
            id={id}
            isGroup={data.childCount !== undefined && data.childCount > 0}
            isOpen={data.isOpen}
            label={data.label}
          />
        </Button>
      </Flex>
    </NodeWrapper>
  );
};
