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

import type { CSSProperties } from "react";

/**
 * Returns ReactFlow style props that use Chakra UI CSS variables
 *
 * @param colorMode - Current color mode (light/dark)
 * @returns Style object to pass to ReactFlow's style prop
 *
 * @example
 * ```tsx
 * import { getReactFlowThemeStyle } from "src/theme/reactflow";
 *
 * const { colorMode } = useColorMode();
 *
 * <ReactFlow
 *   style={getReactFlowThemeStyle(colorMode)}
 *   nodes={nodes}
 *   edges={edges}
 * />
 * ```
 */
export const getReactFlowThemeStyle = (colorMode: "light" | "dark"): CSSProperties => ({
  // Background
  "--xy-background-color": colorMode === "dark"
    ? "var(--chakra-colors-steel-950)"
    : "var(--chakra-colors-steel-50)",
  "--xy-background-pattern-color": colorMode === "dark"
    ? "var(--chakra-colors-gray-200)"
    : "var(--chakra-colors-gray-800)",

  // Nodes
  // "--xy-node-color": "var(--chakra-colors-fg)",
  // "--xy-node-border": "var(--chakra-colors-gray-emphasized)",
  // "--xy-node-background": colorMode === "dark"
  //   ? "var(--chakra-colors-gray-900)"
  //   : "var(--chakra-colors-white)",
  // "--xy-node-group-background": colorMode === "dark"
  //   ? "var(--chakra-colors-gray-800)"
  //   : "var(--chakra-colors-gray-50)",
  // "--xy-node-boxshadow-hover": colorMode === "dark"
  //   ? "0 1px 4px 1px var(--chakra-colors-gray-700)"
  //   : "0 1px 4px 1px var(--chakra-colors-gray-300)",
  // "--xy-node-boxshadow-selected": "0 0 0 2px var(--chakra-colors-blue-focusRing)",

  // Node text
  // "--xy-node-label": "var(--chakra-colors-fg)",

  // Handles
  // "--xy-handle": "var(--chakra-colors-gray-emphasized)",
  // "--xy-handle-border": "var(--chakra-colors-gray-emphasized)",
  // "--xy-handle-border-hover": "var(--chakra-colors-blue-solid)",
  // "--xy-handle-border-connecting": "var(--chakra-colors-blue-solid)",
  // "--xy-handle-border-valid": "var(--chakra-colors-green-solid)",
  // "--xy-handle-border-invalid": "var(--chakra-colors-red-solid)",

  // Edges
  // "--xy-edge-stroke": "var(--chakra-colors-gray-emphasized)",
  // "--xy-edge-stroke-hover": "var(--chakra-colors-blue-solid)",
  // "--xy-edge-stroke-selected": "var(--chakra-colors-blue-solid)",
  // "--xy-edge-stroke-width": "1px",
  // "--xy-edge-stroke-width-hover": "2px",
  // "--xy-edge-stroke-width-selected": "2px",
  // "--xy-edge-label-color": "var(--chakra-colors-fg)",
  // "--xy-edge-label-background": colorMode === "dark"
  //   ? "var(--chakra-colors-black)"
  //   : "var(--chakra-colors-white)",

  // Connection line
  // "--xy-connectionline-stroke": "var(--chakra-colors-blue-solid)",
  // "--xy-connectionline-stroke-width": "2px",

  // Controls
  // "--xy-controls-button-background": colorMode === "dark"
  //   ? "var(--chakra-colors-gray-800)"
  //   : "var(--chakra-colors-white)",
  // "--xy-controls-button-background-hover": colorMode === "dark"
  //   ? "var(--chakra-colors-gray-700)"
  //   : "var(--chakra-colors-gray-50)",
  // "--xy-controls-button-color": "var(--chakra-colors-fg)",
  // "--xy-controls-button-color-hover": "var(--chakra-colors-fg)",
  // "--xy-controls-button-border": "var(--chakra-colors-gray-emphasized)",
  // "--xy-controls-box-shadow": colorMode === "dark"
  //   ? "0 2px 4px var(--chakra-colors-gray-900)"
  //   : "0 2px 4px var(--chakra-colors-gray-200)",

  // MiniMap
  // "--xy-minimap-background": colorMode === "dark"
  //   ? "var(--chakra-colors-black)"
  //   : "var(--chakra-colors-white)",
  // "--xy-minimap-mask": colorMode === "dark"
  //   ? "var(--chakra-colors-gray-600)"
  //   : "var(--chakra-colors-gray-300)",
  // "--xy-minimap-node": "var(--chakra-colors-gray-emphasized)",
  // Additional minimap variables that might be needed
  // "--xy-minimap-background-color": colorMode === "dark"
  //   ? "var(--chakra-colors-black)"
  //   : "var(--chakra-colors-white)",
  // Override any potential default dark colors
  backgroundColor: colorMode === "dark"
    ? "var(--chakra-colors-black)"
    : "var(--chakra-colors-white)",

  // Selection
  // "--xy-selection-background": "var(--chakra-colors-blue-subtle)",
  // "--xy-selection-border": "var(--chakra-colors-blue-solid)",

  // Panel
  // "--xy-panel-background": colorMode === "dark"
  //   ? "var(--chakra-colors-gray-800)"
  //   : "var(--chakra-colors-white)",
  // "--xy-panel-border": "var(--chakra-colors-gray-emphasized)",

  // Attribution
  // "--xy-attribution-background": colorMode === "dark"
  //   ? "var(--chakra-colors-gray-800)"
  //   : "var(--chakra-colors-white)",
  // "--xy-attribution-color": "var(--chakra-colors-gray-muted)",

  // Resize handles
  // "--xy-resize-handle": "var(--chakra-colors-blue-solid)",
  // "--xy-resize-handle-background": colorMode === "dark"
  //   ? "var(--chakra-colors-black)"
  //   : "var(--chakra-colors-white)",

  // Node Toolbar
  // "--xy-node-toolbar-background": colorMode === "dark"
  //   ? "var(--chakra-colors-gray-800)"
  //   : "var(--chakra-colors-white)",
  // "--xy-node-toolbar-border": "var(--chakra-colors-gray-emphasized)",
  // "--xy-node-toolbar-box-shadow": colorMode === "dark"
  //   ? "0 2px 4px var(--chakra-colors-gray-900)"
  //   : "0 2px 4px var(--chakra-colors-gray-200)",
} as CSSProperties);
