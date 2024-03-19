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

/* global ResizeObserver */

import React, { useRef, useEffect } from "react";
import { Table, Tbody, Box, Thead, IconButton } from "@chakra-ui/react";

import { MdDoubleArrow } from "react-icons/md";

import { useGridData } from "src/api";
import { useOffsetTop } from "src/utils";

import renderTaskRows from "./renderTaskRows";
import DagRuns from "./dagRuns";
import useSelection from "../useSelection";

interface Props {
  isPanelOpen?: boolean;
  onPanelToggle?: () => void;
  hoveredTaskState?: string | null;
  openGroupIds: string[];
  onToggleGroups: (groupIds: string[]) => void;
  isGridCollapsed?: boolean;
  setIsGridCollapsed?: (collapsed: boolean) => void;
  gridScrollRef?: React.RefObject<HTMLDivElement>;
  ganttScrollRef?: React.RefObject<HTMLDivElement>;
}

const Grid = ({
  isPanelOpen = false,
  onPanelToggle,
  hoveredTaskState,
  openGroupIds,
  onToggleGroups,
  isGridCollapsed,
  setIsGridCollapsed,
  gridScrollRef,
  ganttScrollRef,
}: Props) => {
  const tableRef = useRef<HTMLTableSectionElement>(null);
  const offsetTop = useOffsetTop(tableRef);
  const { selected } = useSelection();

  const {
    data: { groups, dagRuns },
  } = useGridData();
  const dagRunIds = dagRuns
    .map((dr) => dr.runId)
    .filter((id, i) => {
      if (isGridCollapsed) {
        if (selected.runId) return id === selected.runId;
        return i === dagRuns.length - 1;
      }
      return true;
    });

  const onGanttScroll = (e: Event) => {
    const { scrollTop } = e.currentTarget as HTMLDivElement;
    if (scrollTop && gridScrollRef?.current) {
      gridScrollRef.current.scrollTo(0, scrollTop);

      // Double check the scroll position after 100ms
      setTimeout(() => {
        const gridScrollTop = gridScrollRef?.current?.scrollTop;
        const ganttScrollTop = ganttScrollRef?.current?.scrollTop;
        if (ganttScrollTop !== gridScrollTop && gridScrollRef?.current) {
          gridScrollRef.current.scrollTo(0, ganttScrollTop || 0);
        }
      }, 100);
    }
  };

  // Sync grid and gantt scroll
  useEffect(() => {
    const gantt = ganttScrollRef?.current;
    gantt?.addEventListener("scroll", onGanttScroll);
    return () => {
      gantt?.removeEventListener("scroll", onGanttScroll);
    };
  });

  useEffect(() => {
    const scrollOnResize = new ResizeObserver(() => {
      const runsContainer = gridScrollRef?.current;
      // Set scroll to top right if it is scrollable
      if (
        tableRef?.current &&
        runsContainer &&
        runsContainer.scrollWidth > runsContainer.clientWidth
      ) {
        runsContainer.scrollBy(tableRef.current.offsetWidth, 0);
      }
    });

    if (tableRef && tableRef.current) {
      const table = tableRef.current;

      scrollOnResize.observe(table);
      return () => {
        scrollOnResize.unobserve(table);
      };
    }
    return () => {};
  }, [tableRef, isGridCollapsed, gridScrollRef]);

  return (
    <Box height="100%" position="relative">
      {(isPanelOpen || isGridCollapsed) && (
        <IconButton
          fontSize="2xl"
          variant="ghost"
          color="gray.400"
          size="sm"
          position="absolute"
          right={isGridCollapsed ? -10 : 0}
          zIndex={2}
          top={-8}
          onClick={() =>
            setIsGridCollapsed && setIsGridCollapsed(!isGridCollapsed)
          }
          title={isGridCollapsed ? "Restore grid" : "Collapse grid"}
          aria-label={isGridCollapsed ? "Restore grid" : "Collapse grid"}
          icon={<MdDoubleArrow />}
          transform={isGridCollapsed ? undefined : "rotateZ(180deg)"}
          transitionProperty="none"
        />
      )}
      {!isGridCollapsed && (
        <IconButton
          fontSize="2xl"
          variant="ghost"
          color="gray.400"
          size="sm"
          position="absolute"
          right={isPanelOpen ? -10 : 0}
          zIndex={2}
          top={-8}
          onClick={onPanelToggle}
          title={`${isPanelOpen ? "Hide" : "Show"} Details Panel`}
          aria-label={isPanelOpen ? "Show Details" : "Hide Details"}
          icon={<MdDoubleArrow />}
          transform={isPanelOpen ? undefined : "rotateZ(180deg)"}
          transitionProperty="none"
        />
      )}
      <Box
        maxHeight={`calc(100% - ${offsetTop}px)`}
        ref={gridScrollRef}
        overflow="auto"
        position="relative"
        mt={8}
        overscrollBehavior="auto"
        pb={4}
      >
        <Table borderRightWidth="16px" borderColor="transparent">
          <Thead>
            <DagRuns
              groups={groups}
              openGroupIds={openGroupIds}
              onToggleGroups={onToggleGroups}
              isGridCollapsed={isGridCollapsed}
            />
          </Thead>
          <Tbody ref={tableRef}>
            {renderTaskRows({
              task: groups,
              dagRunIds,
              openGroupIds,
              onToggleGroups,
              hoveredTaskState,
              isGridCollapsed,
            })}
          </Tbody>
        </Table>
      </Box>
    </Box>
  );
};

export default Grid;
