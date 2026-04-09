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

import { Flex, HStack, Text } from "@chakra-ui/react";
import type { NodeProps, Node as NodeType } from "@xyflow/react";
import { memo } from "react";
import { useTranslation } from "react-i18next";
import { FiZap } from "react-icons/fi";

import { NodeWrapper } from "./NodeWrapper";
import type { CustomNodeProps } from "./reactflowUtils";

/**
 * Trigger node component for DAG graph visualization.
 *
 * Displays a trigger task node in the DAG graph with a lightning bolt icon,
 * node label, and descriptive subtitle. Supports internationalization for
 * 21 languages.
 *
 * @component
 * @example
 * <TriggerNode
 *   data={{ label: "Manual Trigger", height: 100, width: 150, isSelected: false }}
 *   id="trigger-1"
 * />
 *
 * @param {NodeProps<NodeType<CustomNodeProps, "trigger">>} props
 * @param {Object} props.data - Node data object
 * @param {number} [props.data.height=100] - Node height in pixels (clamped: 40-1000)
 * @param {number} [props.data.width=100] - Node width in pixels (clamped: 40-1000)
 * @param {string} [props.data.label="Unknown"] - Trigger node label/name
 * @param {boolean} [props.data.isSelected=false] - Whether the node is selected
 * @returns {React.ReactElement} Rendered trigger node component
 */
export const TriggerNode = memo(
  ({
    data: {
      height = 100,
      isSelected = false,
      label = "Unknown",
      width = 100,
    },
  }: NodeProps<NodeType<CustomNodeProps, "trigger">>) => {
    const { t } = useTranslation("components");

    // Validate and sanitize inputs
    const validHeight = Math.max(40, Math.min(Math.floor(height || 100), 1000));
    const validWidth = Math.max(40, Math.min(Math.floor(width || 100), 1000));
    const validLabel = (label || "")
      .trim()
      .slice(0, 100) || "Unknown"; // Max 100 chars, prevent XSS

    // Get translated text with fallback
    const triggerLabel = t("graph.triggerDagRun", {
      defaultValue: "Trigger DAG Run",
    });

    return (
      <NodeWrapper>
        <Flex
          bg="bg"
          borderColor={isSelected ? "border.inverted" : "border"}
          borderRadius={5}
          borderWidth={isSelected ? 4 : 2}
          cursor="default"
          flexDirection="column"
          height={validHeight}
          px={3}
          py={1}
          width={validWidth}
          role="article"
          aria-selected={isSelected}
          aria-label={`${validLabel} trigger node, ${isSelected ? "selected" : "not selected"}`}
          data-testid="trigger-node"
          data-label={validLabel}
          data-selected={isSelected}
        >
          <HStack alignItems="center" data-testid="trigger-node-header">
            <FiZap
              aria-hidden="true"
              data-testid="trigger-icon"
              title="Trigger node indicator"
            />
            <Text
              fontWeight="medium"
              overflow="hidden"
              textOverflow="ellipsis"
              whiteSpace="nowrap"
              title={validLabel}
              data-testid="trigger-label"
            >
              {validLabel}
            </Text>
          </HStack>
          <Text
            color="fg.muted"
            fontSize="sm"
            data-testid="trigger-description"
            title={triggerLabel}
          >
            {triggerLabel}
          </Text>
        </Flex>
      </NodeWrapper>
    );
  },
  (prevProps, nextProps) => {
    // Custom comparison for memoization
    // Return true if props are equal (skip re-render)
    return (
      prevProps.data.label === nextProps.data.label &&
      prevProps.data.height === nextProps.data.height &&
      prevProps.data.width === nextProps.data.width &&
      prevProps.data.isSelected === nextProps.data.isSelected &&
      prevProps.id === nextProps.id
    );
  }
);

// Display name for debugging in React DevTools
TriggerNode.displayName = "TriggerNode";
