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

import type { DagRunState, DagRunType, GridRunsResponse } from "openapi/requests";
import { useOpenGroups } from "src/context/openGroups";
import { useNavigation } from "src/hooks/navigation";
import { useGridRuns } from "src/queries/useGridRuns.ts";
import { useGridStructure } from "src/queries/useGridStructure.ts";
import { isStatePending } from "src/utils";

import { Bar } from "./Bar";
import { DurationAxis } from "./DurationAxis";
import { DurationTick } from "./DurationTick";
import { TaskNames } from "./TaskNames";
import { flattenNodes } from "./utils";

dayjs.extend(dayjsDuration);

type Props = {
  readonly dagRunState?: DagRunState | undefined;
  readonly limit: number;
  readonly runType?: DagRunType | undefined;
  readonly showGantt?: boolean;
  readonly triggeringUser?: string | undefined;
};

export const Grid = ({ dagRunState, limit, runType, showGantt, triggeringUser }: Props) => {
  const { t: translate } = useTranslation("dag");
  const gridRef = useRef<HTMLDivElement>(null);

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
    includeDownstream,
    includeUpstream,
    limit,
    root: filterRoot,
    runType,
    triggeringUser,
  });

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
    return flattenNodes(dagStructure, openGroupIds);
  }, [dagStructure, openGroupIds]);

  const { setMode } = useNavigation({
    onToggleGroup: toggleGroupId,
    runs: gridRuns ?? [],
    tasks: flatNodes,
  });

  return (
    <Flex
      justifyContent="flex-start"
      outline="none"
      position="relative"
      pt={20}
      ref={gridRef}
      tabIndex={0}
      width={showGantt ? "1/2" : "full"}
    >
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
          <Flex flexDirection="row-reverse">
            {gridRuns?.map((dr: GridRunsResponse) => (
              <Bar
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
