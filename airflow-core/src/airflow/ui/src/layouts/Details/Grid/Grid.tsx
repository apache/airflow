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
import { useMemo } from "react";
import { FiChevronsRight } from "react-icons/fi";
import { Link, useParams } from "react-router-dom";

import { useOpenGroups } from "src/context/openGroups";
import { useGrid } from "src/queries/useGrid";

import { Bar } from "./Bar";
import { DurationAxis } from "./DurationAxis";
import { DurationTick } from "./DurationTick";
import { TaskNames } from "./TaskNames";
import { flattenNodes, type RunWithDuration } from "./utils";

dayjs.extend(dayjsDuration);

type Props = {
  readonly limit: number;
};

export const Grid = ({ limit }: Props) => {
  const { openGroupIds } = useOpenGroups();
  const { dagId = "" } = useParams();

  const { data: gridData, isLoading, runAfter } = useGrid(limit);

  const runs: Array<RunWithDuration> = useMemo(
    () =>
      (gridData?.dag_runs ?? []).map((run) => {
        const duration = dayjs
          .duration(dayjs(run.end_date ?? undefined).diff(run.start_date ?? undefined))
          .asSeconds();

        return {
          ...run,
          duration,
        };
      }),
    [gridData?.dag_runs],
  );

  // calculate dag run bar heights relative to max
  const max = Math.max.apply(
    undefined,
    runs.map((dr) => dr.duration),
  );

  const { flatNodes } = useMemo(
    () => flattenNodes(gridData === undefined ? [] : gridData.structure.nodes, openGroupIds),
    [gridData, openGroupIds],
  );

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
            {Boolean(runs.length) && (
              <>
                <DurationTick bottom="92px">{Math.floor(max)}s</DurationTick>
                <DurationTick bottom="46px">{Math.floor(max / 2)}s</DurationTick>
                <DurationTick bottom="-4px">0s</DurationTick>
              </>
            )}
          </Flex>
          <Flex flexDirection="row-reverse">
            {runs.map((dr) => (
              <Bar key={dr.dag_run_id} max={max} nodes={flatNodes} run={dr} />
            ))}
          </Flex>
          {runAfter === undefined ? undefined : (
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
