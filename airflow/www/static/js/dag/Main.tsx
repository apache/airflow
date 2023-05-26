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

/* global localStorage */

import React, { useState, useRef, useEffect, useCallback } from "react";
import { Box, Flex, Divider, Spinner, useDisclosure } from "@chakra-ui/react";
import { isEmpty, debounce } from "lodash";

import { useGridData } from "src/api";
import { hoverDelay } from "src/utils";

import Details from "./details";
import Grid from "./grid";
import FilterBar from "./nav/FilterBar";
import LegendRow from "./nav/LegendRow";
import useToggleGroups from "./useToggleGroups";

const detailsPanelKey = "hideDetailsPanel";
const minPanelWidth = 300;
const collapsedWidth = "28px";

const gridWidthKey = "grid-width";
const saveWidth = debounce(
  (w) => localStorage.setItem(gridWidthKey, w),
  hoverDelay
);

const footerHeight =
  parseInt(
    getComputedStyle(
      document.getElementsByTagName("body")[0]
    ).paddingBottom.replace("px", ""),
    10
  ) || 0;
const headerHeight =
  parseInt(
    getComputedStyle(
      document.getElementsByTagName("body")[0]
    ).paddingTop.replace("px", ""),
    10
  ) || 0;

const Main = () => {
  const {
    data: { groups },
    isLoading,
  } = useGridData();
  const [isGridCollapsed, setIsGridCollapsed] = useState(false);
  const resizeRef = useRef<HTMLDivElement>(null);
  const gridRef = useRef<HTMLDivElement>(null);
  const isPanelOpen = localStorage.getItem(detailsPanelKey) !== "true";
  const { isOpen, onToggle } = useDisclosure({ defaultIsOpen: isPanelOpen });
  const [hoveredTaskState, setHoveredTaskState] = useState<
    string | null | undefined
  >();
  const { openGroupIds, onToggleGroups } = useToggleGroups();

  // Add a debounced delay to not constantly trigger highlighting certain task states
  const onStatusHover = debounce(
    (state) => setHoveredTaskState(state),
    hoverDelay
  );

  const onStatusLeave = () => {
    setHoveredTaskState(undefined);
    onStatusHover.cancel();
  };

  const gridWidth = localStorage.getItem(gridWidthKey) || undefined;

  const onPanelToggle = () => {
    if (!isOpen) {
      localStorage.setItem(detailsPanelKey, "false");
    } else {
      localStorage.setItem(detailsPanelKey, "true");
      if (isGridCollapsed) {
        setIsGridCollapsed(!isGridCollapsed);
      }
    }
    onToggle();
  };

  const resize = useCallback(
    (e: MouseEvent) => {
      const gridEl = gridRef.current;
      if (
        gridEl &&
        e.x > minPanelWidth &&
        e.x < window.innerWidth - minPanelWidth
      ) {
        const width = `${e.x}px`;
        gridEl.style.width = width;
        saveWidth(width);
      }
    },
    [gridRef]
  );

  useEffect(() => {
    const resizeEl = resizeRef.current;
    if (resizeEl) {
      resizeEl.addEventListener("mousedown", (e) => {
        e.preventDefault();
        document.addEventListener("mousemove", resize);
      });

      document.addEventListener("mouseup", () => {
        document.removeEventListener("mousemove", resize);
      });

      return () => {
        resizeEl?.removeEventListener("mousedown", resize);
        document.removeEventListener("mouseup", resize);
      };
    }
    return () => {};
  }, [resize, isLoading, isOpen]);

  const onToggleGridCollapse = () => {
    const gridElement = gridRef.current;
    if (gridElement) {
      if (isGridCollapsed) {
        gridElement.style.width = localStorage.getItem(gridWidthKey) || "";
      } else {
        gridElement.style.width = collapsedWidth;
      }
      setIsGridCollapsed(!isGridCollapsed);
    }
  };

  return (
    <Box
      flex={1}
      height={`calc(100vh - ${footerHeight + headerHeight}px)`}
      maxHeight={`calc(100vh - ${footerHeight + headerHeight}px)`}
      minHeight="750px"
      overflow="hidden"
      position="relative"
    >
      <FilterBar />
      <LegendRow onStatusHover={onStatusHover} onStatusLeave={onStatusLeave} />
      <Divider mb={5} borderBottomWidth={2} />
      <Flex height="100%">
        {isLoading || isEmpty(groups) ? (
          <Spinner />
        ) : (
          <>
            <Box
              flex={isOpen ? undefined : 1}
              minWidth={isGridCollapsed ? collapsedWidth : minPanelWidth}
              ref={gridRef}
              height="100%"
              width={isGridCollapsed ? collapsedWidth : gridWidth}
            >
              <Grid
                isPanelOpen={isOpen}
                onPanelToggle={onPanelToggle}
                hoveredTaskState={hoveredTaskState}
                openGroupIds={openGroupIds}
                onToggleGroups={onToggleGroups}
                isGridCollapsed={isGridCollapsed}
                setIsGridCollapsed={onToggleGridCollapse}
              />
            </Box>
            {isOpen && (
              <>
                <Box
                  width={2}
                  cursor="ew-resize"
                  bg="gray.200"
                  ref={resizeRef}
                  zIndex={1}
                />
                <Box
                  flex={1}
                  minWidth={minPanelWidth}
                  zIndex={1}
                  bg="white"
                  height="100%"
                >
                  <Details
                    openGroupIds={openGroupIds}
                    onToggleGroups={onToggleGroups}
                    hoveredTaskState={hoveredTaskState}
                  />
                </Box>
              </>
            )}
          </>
        )}
      </Flex>
    </Box>
  );
};

export default Main;
