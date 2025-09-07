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
import { useLocalStorage } from "usehooks-ts";

import { useStructureServiceStructureData } from "openapi/queries";
import type { GridRunsResponse } from "openapi/requests";
import { useOpenGroups } from "src/context/openGroups";
import { useNavigation } from "src/hooks/navigation";
import useSelectedVersion from "src/hooks/useSelectedVersion";
import { useGridRuns } from "src/queries/useGridRuns.ts";
import { useGridStructure } from "src/queries/useGridStructure.ts";
import { isStatePending } from "src/utils";

import { Bar } from "./Bar";
import { DurationAxis } from "./DurationAxis";
import { DurationTick } from "./DurationTick";
import { TaskNames } from "./TaskNames";
import { buildEdges, filterNodesByDirection, flattenNodes } from "./utils";

dayjs.extend(dayjsDuration);

type Props = {
  readonly limit: number;
  readonly showGantt?: boolean;
};

export const Grid = ({ limit, showGantt }: Props) => {
  const { t: translate } = useTranslation("dag");
  const gridRef = useRef<HTMLDivElement>(null);

  const [selectedIsVisible, setSelectedIsVisible] = useState<boolean | undefined>();
  const [hasActiveRun, setHasActiveRun] = useState<boolean | undefined>();
  const { openGroupIds, toggleGroupId } = useOpenGroups();
  const { dagId = "", runId = "", taskId } = useParams();

  const { data: gridRuns, isLoading } = useGridRuns({ limit });

  const [searchParams] = useSearchParams();
  const rawTaskFilter = (searchParams.get("task_filter") ?? undefined) as
    | "all"
    | "both"
    | "downstream"
    | "upstream"
    | null;
  // default to "all" so "no param" -> show full DAG
  const taskFilter = rawTaskFilter ?? "all";

  // backward-compat: you had dependencies localStorage; keep it
  const selectedVersion = useSelectedVersion();
  const [dependencies] = useLocalStorage<"all" | "immediate" | "tasks">(`dependencies-${dagId}`, "tasks");

  // derive server flags
  const includeUpstream = taskFilter === "upstream" || taskFilter === "both";
  const includeDownstream = taskFilter === "downstream" || taskFilter === "both";

  type StructurePruneParams = {
    includeDownstream?: boolean;
    includeUpstream?: boolean;
    root?: string;
  };

  // Only include server pruning params when filter !== 'all' and a root is provided
  const structureParams: StructurePruneParams =
    taskFilter !== "all" && taskId !== undefined && taskId !== ""
      ? {
          includeDownstream,
          includeUpstream,
          root: taskId,
        }
      : {};

  const { data: structureData = { edges: [], nodes: [] } } = useStructureServiceStructureData(
    {
      dagId,
      externalDependencies: dependencies === "immediate",
      versionNumber: selectedVersion,
      ...structureParams,
    } as unknown as Parameters<typeof useStructureServiceStructureData>[0],
    undefined,
    { enabled: selectedVersion !== undefined },
  );

  // Check if the selected dag run is inside of the grid response, if not, we'll update the grid filters
  useEffect(() => {
    if (gridRuns && runId) {
      const run = gridRuns.find((dr: GridRunsResponse) => dr.run_id === runId);

      if (!run) {
        setSelectedIsVisible(false);
      }
    }
  }, [runId, gridRuns, selectedIsVisible, setSelectedIsVisible]);

  useEffect(() => {
    if (gridRuns) {
      const run = gridRuns.some((dr: GridRunsResponse) => isStatePending(dr.state));

      if (!run) {
        setHasActiveRun(false);
      }
    }
  }, [gridRuns, setHasActiveRun]);

  const { data: dagStructure } = useGridStructure({ hasActiveRun, limit });

  // calculate dag run bar heights relative to max
  const max = Math.max.apply(
    undefined,
    gridRuns === undefined
      ? []
      : gridRuns
          .map((dr: GridRunsResponse) => dr.duration)
          .filter((duration: number | null): duration is number => duration !== null),
  );

  const { flatNodes } = useMemo(() => flattenNodes(dagStructure, openGroupIds), [dagStructure, openGroupIds]);

  // build edges from structure API
  const edges = useMemo(() => buildEdges(structureData.edges), [structureData.edges]);

  // Filter visible nodes for grid based on filter and root
  const filteredNodes = useMemo(
    () =>
      filterNodesByDirection({
        edges,
        filter: taskFilter,
        flatNodes,
        taskId,
      }),
    [flatNodes, edges, taskId, taskFilter],
  );

  const { setMode } = useNavigation({
    onToggleGroup: toggleGroupId,
    runs: gridRuns ?? [],
    tasks: filteredNodes,
  });

  return (
    <Flex
      justifyContent="flex-start"
      outline="none"
      position="relative"
      pt={20}
      ref={gridRef}
      tabIndex={0}
      width={showGantt ? undefined : "100%"}
    >
      <Box flexGrow={1} minWidth={7} position="relative" top="100px">
        <TaskNames nodes={filteredNodes} onRowClick={() => setMode("task")} />
      </Box>
      <Box position="relative">
        <Flex position="relative">
          <DurationAxis top="100px" />
          <DurationAxis top="50px" />
          <DurationAxis top="4px" />
          <Flex
            flexDirection="column-reverse"
            height="100px"
            position="relative"
            width={showGantt ? undefined : "100%"}
          >
            {Boolean(gridRuns?.length) && (
              <>
                <DurationTick bottom="92px" duration={max} />
                <DurationTick bottom="46px" duration={max / 2} />
                <DurationTick bottom="-4px" duration={0} />
              </>
            )}
          </Flex>
          <Flex flexDirection="row-reverse">
            {gridRuns?.map((dr: GridRunsResponse) => (
              <Bar
                key={dr.run_id}
                max={max}
                nodes={filteredNodes}
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
