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
import { Box, Flex, IconButton } from "@chakra-ui/react";
import dayjs from "dayjs";
import dayjsDuration from "dayjs/plugin/duration";
import { useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { FiChevronsRight } from "react-icons/fi";
import { Link, useParams, useSearchParams } from "react-router-dom";

import { useStructureServiceStructureData } from "openapi/queries";
import type { DagRunState, DagRunType, GridRunsResponse } from "openapi/requests";
import { useOpenGroups } from "src/context/openGroups";
import { useNavigation } from "src/hooks/navigation";
import useSelectedVersion from "src/hooks/useSelectedVersion";
import { useGridRuns } from "src/queries/useGridRuns.ts";
import { useGridStructure } from "src/queries/useGridStructure.ts";
import { isStatePending } from "src/utils";
import { setRef } from "src/utils/domUtils";

import { Bar } from "./Bar";
import { DurationAxis } from "./DurationAxis";
import { DurationTick } from "./DurationTick";
import { GridOverlays } from "./GridOverlays";
import { TaskNames } from "./TaskNames";
import { BAR_HEADER_HEIGHT, CELL_HEIGHT, CELL_WIDTH, GRID_PADDING_TOP } from "./utils";
import { flattenNodes } from "./utils";

dayjs.extend(dayjsDuration);

type Props = {
  readonly dagRunState?: DagRunState | undefined;
  readonly ganttHoverRowRef?: React.RefObject<HTMLDivElement>;
  readonly hoverRowRef: React.RefObject<HTMLDivElement>;
  readonly limit: number;
  readonly runType?: DagRunType | undefined;
  readonly showGantt?: boolean;
  readonly triggeringUser?: string | undefined;
};

export const Grid = ({
  dagRunState,
  ganttHoverRowRef,
  hoverRowRef,
  limit,
  runType,
  showGantt,
  triggeringUser,
}: Props) => {
  const { t: translate } = useTranslation("dag");
  const gridRef = useRef<HTMLDivElement>(null);

  // Overlay ref for column highlighting (direct DOM manipulation for zero latency)
  const hoverColRef = useRef<HTMLDivElement>(null);

  // Overlay refs for keyboard navigation highlighting
  const navRowRef = useRef<HTMLDivElement>(null);
  const navColRef = useRef<HTMLDivElement>(null);
  const navCellRef = useRef<HTMLDivElement>(null);

  const [selectedIsVisible, setSelectedIsVisible] = useState<boolean | undefined>();
  const { openGroupIds, toggleGroupId } = useOpenGroups();
  const { dagId = "", runId = "" } = useParams();
  const [searchParams] = useSearchParams();

  const filterRoot = searchParams.get("root") ?? undefined;
  const includeUpstream = searchParams.get("upstream") === "true";
  const includeDownstream = searchParams.get("downstream") === "true";

  const { data: gridRuns, isLoading } = useGridRuns({ dagRunState, limit, runType, triggeringUser });

  // Check if the selected dag run is inside of the grid response, if not, we'll update the grid filters
  // Eventually we should redo the api endpoint to make this work better
  useEffect(() => {
    if (gridRuns && runId) {
      const run = gridRuns.find((dr: GridRunsResponse) => dr.run_id === runId);

      if (!run) {
        setSelectedIsVisible(false);
      }
    }
  }, [runId, gridRuns, selectedIsVisible, setSelectedIsVisible]);

  const { data: dagStructure } = useGridStructure({
    dagRunState,
    hasActiveRun: gridRuns?.some((dr) => isStatePending(dr.state)),
    limit,
    runType,
    triggeringUser,
  });

  const selectedVersion = useSelectedVersion();

  const hasActiveFilter = includeUpstream || includeDownstream;

  // fetch filtered structure when filter is active
  const { data: taskStructure } = useStructureServiceStructureData(
    {
      dagId,
      externalDependencies: false,
      includeDownstream,
      includeUpstream,
      root: hasActiveFilter && filterRoot !== undefined ? filterRoot : undefined,
      versionNumber: selectedVersion,
    },
    undefined,
    {
      enabled: selectedVersion !== undefined && hasActiveFilter && filterRoot !== undefined,
    },
  );

  // extract allowed task IDs from task structure when filter is active
  const allowedTaskIds = useMemo(() => {
    if (!hasActiveFilter || filterRoot === undefined || taskStructure === undefined) {
      return undefined;
    }

    const taskIds = new Set<string>();

    const addNodeAndChildren = <T extends { children?: Array<T> | null; id: string }>(currentNode: T) => {
      taskIds.add(currentNode.id);
      if (currentNode.children) {
        currentNode.children.forEach((child) => addNodeAndChildren(child));
      }
    };

    taskStructure.nodes.forEach((node) => {
      addNodeAndChildren(node);
    });

    return taskIds;
  }, [hasActiveFilter, filterRoot, taskStructure]);

  // calculate dag run bar heights relative to max
  const max = Math.max.apply(
    undefined,
    gridRuns === undefined
      ? []
      : gridRuns
          .map((dr: GridRunsResponse) => dr.duration)
          .filter((duration: number | null): duration is number => duration !== null),
  );

  const { flatNodes } = useMemo(() => {
    const nodes = flattenNodes(dagStructure, openGroupIds);

    // filter nodes based on task stream filter if active
    if (allowedTaskIds !== undefined) {
      return {
        ...nodes,
        flatNodes: nodes.flatNodes.filter((node) => allowedTaskIds.has(node.id)),
      };
    }

    return nodes;
  }, [dagStructure, openGroupIds, allowedTaskIds]);

  const { setMode } = useNavigation({
    containerRef: gridRef,
    navCellRef,
    navColRef,
    navRowRef,
    onToggleGroup: toggleGroupId,
    runs: gridRuns ?? [],
    tasks: flatNodes,
  });

  const handleMouseMove = (event: React.MouseEvent<HTMLDivElement>) => {
    if (!gridRef.current) {
      return;
    }

    // Get mouse position relative to grid container
    const rect = gridRef.current.getBoundingClientRect();
    const mouseX = event.clientX - rect.left;
    const mouseY = event.clientY - rect.top;

    // Calculate indices
    const cellsTopPosition = GRID_PADDING_TOP + BAR_HEADER_HEIGHT;
    const rowIndex = Math.floor((mouseY - cellsTopPosition) / CELL_HEIGHT);

    // Columns are in row-reverse order (rightmost = index 0)
    // Need to calculate from the right edge of the grid
    const totalGridWidth = (gridRuns?.length ?? 0) * CELL_WIDTH;
    const colIndex = Math.floor((totalGridWidth - (mouseX - rect.width + totalGridWidth)) / CELL_WIDTH);

    // Validate and update row highlight (for TaskNames + Cells)
    const validRow = rowIndex >= 0 && rowIndex < flatNodes.length;

    if (validRow) {
      const rowY = rowIndex * CELL_HEIGHT;

      setRef(hoverRowRef, { opacity: "1", transform: `translateY(${rowY}px)` });

      // Also update Gantt's overlay if provided
      if (ganttHoverRowRef) {
        setRef(ganttHoverRowRef, { opacity: "1", transform: `translateY(${rowY}px)` });
      }
    } else {
      setRef(hoverRowRef, { opacity: "0" });
      if (ganttHoverRowRef) {
        setRef(ganttHoverRowRef, { opacity: "0" });
      }
    }

    // Validate and update column highlight (for Bar + Cells)
    const validCol = colIndex >= 0 && colIndex < (gridRuns?.length ?? 0);

    if (validCol) {
      const colX = -colIndex * CELL_WIDTH;

      setRef(hoverColRef, { opacity: "1", transform: `translateX(${colX}px)` });
    } else {
      setRef(hoverColRef, { opacity: "0" });
    }
  };

  const handleMouseLeave = () => {
    setRef(hoverRowRef, { opacity: "0" });
    setRef(hoverColRef, { opacity: "0" });
    if (ganttHoverRowRef) {
      setRef(ganttHoverRowRef, { opacity: "0" });
    }
  };

  return (
    <Flex
      justifyContent="flex-start"
      onMouseLeave={handleMouseLeave}
      onMouseMove={handleMouseMove}
      outline="none"
      position="relative"
      pt={20}
      ref={gridRef}
      tabIndex={0}
      width={showGantt ? "1/2" : "full"}
    >
      <GridOverlays
        gridHeight={flatNodes.length * CELL_HEIGHT}
        hoverColRef={hoverColRef}
        hoverRowRef={hoverRowRef}
        navCellRef={navCellRef}
        navColRef={navColRef}
        navRowRef={navRowRef}
      />

      <Box display="flex" flexDirection="column" flexGrow={1} justifyContent="end" minWidth="200px">
        <TaskNames nodes={flatNodes} onRowClick={() => setMode("task")} />
      </Box>
      <Box position="relative">
        <Flex position="relative">
          <DurationAxis top="100px" />
          <DurationAxis top="50px" />
          <DurationAxis top="4px" />
          <Flex flexDirection="column-reverse" height="100px" position="relative">
            {Boolean(gridRuns?.length) && (
              <>
                <DurationTick bottom="92px" duration={max} />
                <DurationTick bottom="46px" duration={max / 2} />
                <DurationTick bottom="-4px" duration={0} />
              </>
            )}
          </Flex>
          <Flex flexDirection="row-reverse" position="relative">
            {gridRuns?.map((dr: GridRunsResponse, colIndex: number) => (
              <Bar
                colIndex={colIndex}
                key={dr.run_id}
                max={max}
                nodes={flatNodes}
                onCellClick={() => setMode("TI")}
                onColumnClick={() => setMode("run")}
                run={dr}
              />
            ))}
          </Flex>
          {selectedIsVisible === undefined || !selectedIsVisible ? undefined : (
            <Link to={`/dags/${dagId}`}>
              <IconButton
                aria-label={translate("grid.buttons.resetToLatest")}
                height="98px"
                loading={isLoading}
                minW={0}
                ml={1}
                title={translate("grid.buttons.resetToLatest")}
                variant="surface"
                zIndex={1}
              >
                <FiChevronsRight />
              </IconButton>
            </Link>
          )}
        </Flex>
      </Box>
    </Flex>
  );
};
