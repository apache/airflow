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
import {
  Box,
  Flex,
  Spinner,
  useDisclosure,
  IconButton,
  Accordion,
  AccordionItem,
  AccordionButton,
  AccordionPanel,
} from "@chakra-ui/react";
import { isEmpty, debounce } from "lodash";
import { FaExpandArrowsAlt, FaCompressArrowsAlt } from "react-icons/fa";

import { useGridData } from "src/api";
import { hoverDelay } from "src/utils";

import ShortcutCheatSheet from "src/components/ShortcutCheatSheet";
import { useKeysPress } from "src/utils/useKeysPress";

import Details from "./details";
import Grid from "./grid";
import FilterBar from "./nav/FilterBar";
import LegendRow from "./nav/LegendRow";
import useToggleGroups from "./useToggleGroups";
import keyboardShortcutIdentifier from "./keyboardShortcutIdentifier";

const minPanelWidth = 300;
const collapsedWidth = "32px";

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

  const [accordionIndexes, setAccordionIndexes] = useState<Array<number>>([0]);
  const isFilterCollapsed = !accordionIndexes.length;

  const resizeRef = useRef<HTMLDivElement>(null);
  const gridRef = useRef<HTMLDivElement>(null);
  const gridScrollRef = useRef<HTMLDivElement>(null);
  const ganttScrollRef = useRef<HTMLDivElement>(null);

  const [hoveredTaskState, setHoveredTaskState] = useState<
    string | null | undefined
  >();
  const { openGroupIds, onToggleGroups } = useToggleGroups();
  const oldGridElX = useRef(0);

  const {
    onClose: onCloseShortcut,
    isOpen: isOpenShortcut,
    onToggle: onToggleShortcut,
  } = useDisclosure();

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

  const onToggleGridCollapse = useCallback(
    (collapseNext?: boolean) => {
      const gridElement = gridRef.current;
      const collapse =
        collapseNext !== undefined ? collapseNext : !isGridCollapsed;
      if (collapse !== undefined)
        if (gridElement) {
          if (!collapse) {
            gridElement.style.width = localStorage.getItem(gridWidthKey) || "";
          } else {
            gridElement.style.width = collapsedWidth;
          }
          setIsGridCollapsed(collapse);
        }
    },
    [isGridCollapsed]
  );

  const resize = useCallback(
    (e: MouseEvent) => {
      const gridEl = gridRef.current;
      if (gridEl) {
        if (e.x > minPanelWidth && e.x < window.innerWidth - minPanelWidth) {
          const width = `${e.x}px`;
          gridEl.style.width = width;
          saveWidth(width);
        } else if (
          // expand grid if cursor moves right
          e.x < minPanelWidth &&
          oldGridElX &&
          oldGridElX.current &&
          oldGridElX.current < e.x
        ) {
          setIsGridCollapsed(false);
        } else if (
          // collapse grid if cursor moves left
          e.x < minPanelWidth / 2 &&
          oldGridElX &&
          oldGridElX.current &&
          oldGridElX.current > e.x
        ) {
          onToggleGridCollapse();
        }
      }
      oldGridElX.current = e.x;
    },
    [gridRef, onToggleGridCollapse]
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
  }, [resize, isLoading]);

  useKeysPress(
    keyboardShortcutIdentifier.toggleShortcutCheatSheet,
    onToggleShortcut
  );

  const isFullScreen = isFilterCollapsed && isGridCollapsed;
  const toggleFullScreen = () => {
    if (!isFullScreen) {
      setAccordionIndexes([]);
      onToggleGridCollapse(true);
    } else {
      setAccordionIndexes([0]);
      onToggleGridCollapse(false);
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
      <Accordion allowToggle index={accordionIndexes} borderTopWidth={0}>
        <AccordionItem
          sx={{
            // Override chakra-collapse so our dropdowns still work
            ".chakra-collapse": {
              overflow: "visible !important",
            },
          }}
        >
          <AccordionButton display="none" />
          <AccordionPanel p={0}>
            <FilterBar />
            <LegendRow
              onStatusHover={onStatusHover}
              onStatusLeave={onStatusLeave}
            />
          </AccordionPanel>
        </AccordionItem>
      </Accordion>
      <Flex height="100%">
        {isLoading || isEmpty(groups) ? (
          <Spinner />
        ) : (
          <>
            <Box
              minWidth={isGridCollapsed ? collapsedWidth : minPanelWidth}
              ref={gridRef}
              height="100%"
              width={isGridCollapsed ? collapsedWidth : gridWidth}
              position="relative"
            >
              <IconButton
                icon={
                  isFullScreen ? <FaExpandArrowsAlt /> : <FaCompressArrowsAlt />
                }
                fontSize="xl"
                position="absolute"
                right={0}
                top={0}
                variant="ghost"
                color="gray.400"
                size="sm"
                aria-label="Toggle full screen details"
                title="Toggle full screen details"
                onClick={toggleFullScreen}
              />
              <Grid
                hoveredTaskState={hoveredTaskState}
                openGroupIds={openGroupIds}
                onToggleGroups={onToggleGroups}
                isGridCollapsed={isGridCollapsed}
                gridScrollRef={gridScrollRef}
                ganttScrollRef={ganttScrollRef}
              />
            </Box>
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
                gridScrollRef={gridScrollRef}
                ganttScrollRef={ganttScrollRef}
              />
            </Box>
          </>
        )}
      </Flex>
      <ShortcutCheatSheet
        isOpen={isOpenShortcut}
        onClose={onCloseShortcut}
        header="Shortcuts to interact with DAGs and Tasks"
        keyboardShortcutIdentifier={keyboardShortcutIdentifier}
      />
    </Box>
  );
};

export default Main;
