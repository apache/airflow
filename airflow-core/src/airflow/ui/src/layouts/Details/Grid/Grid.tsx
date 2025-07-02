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
import { useEffect, useMemo, useState } from "react";
import { FiChevronsRight } from "react-icons/fi";
import { Link, useParams } from "react-router-dom";

import type { GridRunsResponse } from "openapi/requests";
import { useOpenGroups } from "src/context/openGroups";
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
  readonly limit: number;
};

export const Grid = ({ limit }: Props) => {
  const [selectedIsVisible, setSelectedIsVisible] = useState<boolean | undefined>();
  const [hasActiveRun, setHasActiveRun] = useState<boolean | undefined>();
  const { openGroupIds } = useOpenGroups();
  const { dagId = "", runId = "" } = useParams();

  const { data: gridRuns, isLoading } = useGridRuns({ limit });

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

  return (
    <Flex justifyContent="flex-start" position="relative" pt={50} width="100%">
      <Box flexGrow={1} minWidth={7} position="relative" top="100px">
        <TaskNames nodes={flatNodes} />
      </Box>
      <Box position="relative">
        <Flex position="relative">
          <DurationAxis top="100px" />
          <DurationAxis top="50px" />
          <DurationAxis top="4px" />
          <Flex flexDirection="column-reverse" height="100px" position="relative" width="100%">
            {Boolean(gridRuns?.length) && (
              <>
                <DurationTick bottom="92px">{Math.floor(max)}s</DurationTick>
                <DurationTick bottom="46px">{Math.floor(max / 2)}s</DurationTick>
                <DurationTick bottom="-4px">0s</DurationTick>
              </>
            )}
          </Flex>
          <Flex flexDirection="row-reverse">
            {gridRuns?.map((dr: GridRunsResponse) => (
              <Bar key={dr.run_id} max={max} nodes={flatNodes} run={dr} />
            ))}
          </Flex>
          {selectedIsVisible === undefined || !selectedIsVisible ? undefined : (
            <Link to={`/dags/${dagId}`}>
              <IconButton
                aria-label="Reset to latest"
                height="98px"
                loading={isLoading}
                minW={0}
                ml={1}
                title="Reset to latest"
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
