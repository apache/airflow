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
import { MdDoubleArrow } from "react-icons/md";
import { useSearchParams } from "react-router-dom";

import { useGridData } from "src/api";
import { hoverDelay } from "src/utils";

import ShortcutCheatSheet from "src/components/ShortcutCheatSheet";
import { useKeysPress } from "src/utils/useKeysPress";

import Details, { TAB_PARAM } from "./details";
import Grid from "./grid";
import FilterBar from "./nav/FilterBar";
import LegendRow from "./nav/LegendRow";
import useToggleGroups from "./useToggleGroups";
import keyboardShortcutIdentifier from "./keyboardShortcutIdentifier";

const detailsPanelKey = "hideDetailsPanel";
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
  const toggleFilterCollapsed = () => {
    if (isFilterCollapsed) setAccordionIndexes([0]);
    else setAccordionIndexes([]);
  };

  const [searchParams] = useSearchParams();
  const resizeRef = useRef<HTMLDivElement>(null);
  const gridRef = useRef<HTMLDivElement>(null);
  const gridScrollRef = useRef<HTMLDivElement>(null);
  const ganttScrollRef = useRef<HTMLDivElement>(null);

  const isPanelOpen =
    localStorage.getItem(detailsPanelKey) !== "true" ||
    !!searchParams.get(TAB_PARAM);
  const { isOpen, onToggle } = useDisclosure({ defaultIsOpen: isPanelOpen });
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

  const onToggleGridCollapse = useCallback(() => {
    const gridElement = gridRef.current;
    if (gridElement) {
      if (isGridCollapsed) {
        gridElement.style.width = localStorage.getItem(gridWidthKey) || "";
      } else {
        gridElement.style.width = collapsedWidth;
      }
      setIsGridCollapsed(!isGridCollapsed);
    }
  }, [isGridCollapsed]);

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
  }, [resize, isLoading, isOpen]);

  useKeysPress(
    keyboardShortcutIdentifier.toggleShortcutCheatSheet,
    onToggleShortcut
  );

  const isFullScreen = isFilterCollapsed && isGridCollapsed;
  const toggleFullScreen = () => {
    if (!isFullScreen) {
      setAccordionIndexes([]);
      setIsGridCollapsed(true);
    } else {
      setAccordionIndexes([0]);
      setIsGridCollapsed(false);
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
      <IconButton
        position="absolute"
        variant="ghost"
        color="gray.400"
        top={0}
        left={0}
        onClick={toggleFilterCollapsed}
        icon={<MdDoubleArrow />}
        aria-label="Toggle filters bar"
        transform={isFilterCollapsed ? "rotateZ(90deg)" : "rotateZ(270deg)"}
        transition="all 0.2s"
      />
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
                gridScrollRef={gridScrollRef}
                ganttScrollRef={ganttScrollRef}
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
                    gridScrollRef={gridScrollRef}
                    ganttScrollRef={ganttScrollRef}
                    isFullScreen={isFullScreen}
                    toggleFullScreen={toggleFullScreen}
                  />
                </Box>
              </>
            )}
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
