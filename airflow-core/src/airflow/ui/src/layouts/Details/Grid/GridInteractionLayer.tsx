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

/**
 * GridInteractionLayer - Overlay-based interaction system for zero-latency hover and navigation
 *
 * Architecture:
 * - Uses transform-based positioning for GPU compositing (no layout/paint triggers)
 * - Operates via refs and direct DOM manipulation (bypasses React render cycle)
 * - Supports both mouse hover and keyboard navigation
 *
 * Hover behavior:
 * - TI (cell) hover: row + column highlight
 * - Task (row) hover: row highlight only
 * - Run (column) hover: column highlight only
 */
import { Box } from "@chakra-ui/react";
import { forwardRef, useCallback, useImperativeHandle, useRef } from "react";

import type { NavigationMode } from "src/hooks/navigation/types";

// Grid cell dimensions (must match actual rendered cell sizes)
export const CELL_WIDTH = 18; // Column width in pixels
export const CELL_HEIGHT = 20; // Row height in pixels

export type HoverMode = "run" | "task" | "TI";

export type GridPosition = {
  colIndex: number;
  rowIndex: number;
  runId?: string;
  taskId?: string;
};

export type GridInteractionLayerHandle = {
  clearHighlight: () => void;
  clearHover: () => void;
  setHighlight: (position: GridPosition, mode: NavigationMode) => void;
  setHover: (position: GridPosition, mode: HoverMode) => void;
};

// Bar header height (duration chart) + Grid padding-top (pt={20} = 80px in Chakra)
const BAR_HEADER_HEIGHT = 100;
const GRID_PADDING_TOP = 80; // pt={20} in Chakra = 20 * 4px = 80px

type Props = {
  readonly gridHeight: number;
  readonly gridWidth: number;
  readonly totalCols: number;
  readonly totalRows: number;
};

export const GridInteractionLayer = forwardRef<GridInteractionLayerHandle, Props>(
  ({ gridHeight, totalCols, totalRows }, ref) => {
    // Navigation highlight refs
    const navRowOverlayRef = useRef<HTMLDivElement>(null);
    const navColOverlayRef = useRef<HTMLDivElement>(null);
    const navCellOverlayRef = useRef<HTMLDivElement>(null);

    // Hover highlight refs (separate from navigation)
    const hoverRowOverlayRef = useRef<HTMLDivElement>(null);
    const hoverColOverlayRef = useRef<HTMLDivElement>(null);

    // Track current position for cleanup
    const currentPosRef = useRef<GridPosition | undefined>();

    // Hide all navigation overlays
    const clearHighlight = useCallback(() => {
      if (navRowOverlayRef.current) {
        navRowOverlayRef.current.style.opacity = "0";
      }
      if (navColOverlayRef.current) {
        navColOverlayRef.current.style.opacity = "0";
      }
      if (navCellOverlayRef.current) {
        navCellOverlayRef.current.style.opacity = "0";
      }
      currentPosRef.current = undefined;
    }, []);

    // Hide all hover overlays
    const clearHover = useCallback(() => {
      if (hoverRowOverlayRef.current) {
        hoverRowOverlayRef.current.style.opacity = "0";
      }
      if (hoverColOverlayRef.current) {
        hoverColOverlayRef.current.style.opacity = "0";
      }
    }, []);

    // Set navigation highlight using transform (GPU compositing)
    const setHighlight = useCallback(
      (position: GridPosition, mode: NavigationMode) => {
        const { colIndex, rowIndex } = position;

        // Validate bounds
        if (rowIndex < 0 || rowIndex >= totalRows || colIndex < 0 || colIndex >= totalCols) {
          return;
        }

        // Calculate positions
        // Row: top-down from cellsTop
        const rowY = rowIndex * CELL_HEIGHT;
        // Column: colIndex 0 is rightmost (newest)
        // Using right positioning with translateX, negative value moves left
        const colX = -colIndex * CELL_WIDTH;

        // Update row highlight (for task/TI modes)
        if (navRowOverlayRef.current && (mode === "task" || mode === "TI")) {
          navRowOverlayRef.current.style.transform = `translateY(${rowY}px)`;
          navRowOverlayRef.current.style.opacity = "1";
        } else if (navRowOverlayRef.current) {
          navRowOverlayRef.current.style.opacity = "0";
        }

        // Update column highlight (for run/TI modes)
        if (navColOverlayRef.current && (mode === "run" || mode === "TI")) {
          navColOverlayRef.current.style.transform = `translateX(${colX}px)`;
          navColOverlayRef.current.style.opacity = "1";
        } else if (navColOverlayRef.current) {
          navColOverlayRef.current.style.opacity = "0";
        }

        // Update cell highlight (for TI mode - intersection point)
        if (navCellOverlayRef.current && mode === "TI") {
          navCellOverlayRef.current.style.transform = `translate(${colX}px, ${rowY}px)`;
          navCellOverlayRef.current.style.opacity = "1";
        } else if (navCellOverlayRef.current) {
          navCellOverlayRef.current.style.opacity = "0";
        }

        currentPosRef.current = position;
      },
      [totalCols, totalRows],
    );

    // Set hover highlight based on mode
    // - TI: row + column
    // - task: row only
    // - run: column only
    const setHover = useCallback(
      (position: GridPosition, mode: HoverMode) => {
        const { colIndex, rowIndex } = position;

        // Validate bounds
        if (rowIndex < 0 || rowIndex >= totalRows || colIndex < 0 || colIndex >= totalCols) {
          clearHover();

          return;
        }

        // Calculate positions
        // Row: top-down from cellsTop
        const rowY = rowIndex * CELL_HEIGHT;
        // Column: colIndex 0 is rightmost, negative translateX moves left
        const colX = -colIndex * CELL_WIDTH;

        // Row highlight (for task/TI hover)
        if (hoverRowOverlayRef.current) {
          if (mode === "task" || mode === "TI") {
            hoverRowOverlayRef.current.style.transform = `translateY(${rowY}px)`;
            hoverRowOverlayRef.current.style.opacity = "1";
          } else {
            hoverRowOverlayRef.current.style.opacity = "0";
          }
        }

        // Column highlight (for run/TI hover)
        if (hoverColOverlayRef.current) {
          if (mode === "run" || mode === "TI") {
            hoverColOverlayRef.current.style.transform = `translateX(${colX}px)`;
            hoverColOverlayRef.current.style.opacity = "1";
          } else {
            hoverColOverlayRef.current.style.opacity = "0";
          }
        }
      },
      [clearHover, totalCols, totalRows],
    );

    // Expose methods to parent via ref
    useImperativeHandle(ref, () => ({ clearHighlight, clearHover, setHighlight, setHover }), [
      clearHighlight,
      clearHover,
      setHighlight,
      setHover,
    ]);

    // Base styles for overlays - using will-change for GPU layer promotion
    const overlayBaseStyle = {
      pointerEvents: "none" as const,
      position: "absolute" as const,
      transition: "transform 0.05s linear, opacity 0.1s ease-out",
      willChange: "transform, opacity",
    };

    // Grid cells start after padding-top + bar header
    const cellsTop = GRID_PADDING_TOP + BAR_HEADER_HEIGHT;

    return (
      <>
        {/* === HOVER OVERLAYS (lower z-index) === */}

        {/* Hover row overlay - full width strip covering TaskNames + Cells */}
        <Box
          bg="blue.500/10"
          height={`${CELL_HEIGHT}px`}
          left={0}
          opacity={0}
          ref={hoverRowOverlayRef}
          top={`${cellsTop}px`}
          width="100%"
          zIndex={1}
          {...overlayBaseStyle}
        />

        {/* Hover column overlay - full height strip */}
        <Box
          bg="blue.500/10"
          height={`${gridHeight + BAR_HEADER_HEIGHT}px`}
          opacity={0}
          ref={hoverColOverlayRef}
          right={0}
          top={`${GRID_PADDING_TOP}px`}
          width={`${CELL_WIDTH}px`}
          zIndex={1}
          {...overlayBaseStyle}
        />

        {/* === NAVIGATION OVERLAYS (higher z-index) === */}

        {/* Navigation row overlay - full width strip covering TaskNames + Cells */}
        <Box
          bg="blue.500/20"
          height={`${CELL_HEIGHT}px`}
          left={0}
          opacity={0}
          ref={navRowOverlayRef}
          top={`${cellsTop}px`}
          width="100%"
          zIndex={2}
          {...overlayBaseStyle}
        />

        {/* Navigation column overlay - full height strip */}
        <Box
          bg="blue.500/20"
          height={`${gridHeight + BAR_HEADER_HEIGHT}px`}
          opacity={0}
          ref={navColOverlayRef}
          right={0}
          top={`${GRID_PADDING_TOP}px`}
          width={`${CELL_WIDTH}px`}
          zIndex={2}
          {...overlayBaseStyle}
        />

        {/* Navigation cell overlay - precise cell indicator */}
        <Box
          border="2px solid"
          borderColor="blue.500"
          borderRadius="sm"
          height={`${CELL_HEIGHT}px`}
          opacity={0}
          ref={navCellOverlayRef}
          right={0}
          top={`${cellsTop}px`}
          width={`${CELL_WIDTH}px`}
          zIndex={3}
          {...overlayBaseStyle}
        />
      </>
    );
  },
);

GridInteractionLayer.displayName = "GridInteractionLayer";
