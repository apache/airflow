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
import { Flex, HStack, LinkOverlay, Text } from "@chakra-ui/react";
import type { NodeProps, Node as NodeType } from "@xyflow/react";
import { Link as RouterLink } from "react-router-dom";

import { useDagServiceGetDag } from "openapi/queries";
import { DagIcon } from "src/assets/DagIcon";
import { TogglePause } from "src/components/TogglePause";

import { NodeWrapper } from "./NodeWrapper";
import { getLineageNodeStyle } from "./lineageStyles";
import type { CustomNodeProps } from "./reactflowUtils";

export const DagNode = ({
  data: { disableNavigation, height, isOpen, isSelected, label, lineageStyle, width },
}: NodeProps<NodeType<CustomNodeProps, "dag">>) => {
  const { data: dag } = useDagServiceGetDag({ dagId: label });
  const nodeStyles = getLineageNodeStyle({
    defaultBackground: isOpen ? "bg.muted" : "bg",
    defaultBorderColor: isSelected ? "border.inverted" : "border",
    defaultBorderWidth: isSelected ? 4 : 2,
    lineageStyle,
  });

  return (
    <NodeWrapper>
      <Flex
        bg={nodeStyles.background}
        borderColor={nodeStyles.borderColor}
        borderRadius={5}
        borderWidth={nodeStyles.borderWidth}
        boxShadow={nodeStyles.boxShadow}
        cursor="default"
        flexDirection="column"
        height={`${height}px`}
        px={3}
        py={1}
        transform={nodeStyles.transform}
        transition="background-color 0.2s, border-color 0.2s, box-shadow 0.2s, transform 0.2s"
        width={`${width}px`}
      >
        <HStack alignItems="center" justifyContent="space-between">
          <DagIcon />
          <TogglePause
            dagId={dag?.dag_id ?? label}
            disabled={!Boolean(dag)}
            isPaused={dag?.is_paused}
            style={{ zIndex: 2 }}
          />
        </HStack>
        {disableNavigation ? (
          <Text>{dag?.dag_display_name ?? label}</Text>
        ) : (
          <LinkOverlay asChild>
            <RouterLink to={`/dags/${dag?.dag_id ?? label}`}>{dag?.dag_display_name ?? label}</RouterLink>
          </LinkOverlay>
        )}
      </Flex>
    </NodeWrapper>
  );
};
