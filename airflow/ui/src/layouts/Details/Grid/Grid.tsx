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
import { keepPreviousData } from "@tanstack/react-query";
import dayjs from "dayjs";
import dayjsDuration from "dayjs/plugin/duration";
import { useMemo } from "react";
import { FiChevronLeft, FiChevronRight } from "react-icons/fi";
import { useParams, useSearchParams } from "react-router-dom";

import { useGridServiceGridData, useStructureServiceStructureData } from "openapi/queries";
import type { GridResponse } from "openapi/requests/types.gen";
import { useOpenGroups } from "src/context/openGroups";
import { isStatePending, useAutoRefresh } from "src/utils";

import { Bar } from "./Bar";
import { DurationAxis } from "./DurationAxis";
import { DurationTick } from "./DurationTick";
import { TaskNames } from "./TaskNames";
import { flattenNodes, type RunWithDuration } from "./utils";

dayjs.extend(dayjsDuration);

const OFFSET_CHANGE = 10;
const limit = 25;

export const Grid = () => {
  const { openGroupIds } = useOpenGroups();
  const { dagId = "" } = useParams();
  const { data: structure } = useStructureServiceStructureData({
    dagId,
  });

  const [searchParams, setSearchParams] = useSearchParams();
  const refetchInterval = useAutoRefresh({ dagId });

  const offset = parseInt(searchParams.get("offset") ?? "0", 10);

  // This is necessary for keepPreviousData
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-arguments
  const { data: gridData } = useGridServiceGridData<GridResponse>(
    {
      dagId,
      limit,
      offset,
      orderBy: "-run_after",
    },
    undefined,
    {
      placeholderData: keepPreviousData,
      refetchInterval: (query) =>
        query.state.data?.dag_runs.some((dr) => isStatePending(dr.state)) && refetchInterval,
    },
  );

  const runs: Array<RunWithDuration> = useMemo(
    () =>
      (gridData?.dag_runs ?? []).map((run) => {
        const duration = dayjs.duration(dayjs(run.end_date).diff(run.start_date)).asSeconds();

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

  const onIncrement = () => {
    if (offset - OFFSET_CHANGE > 0) {
      const newOffset = offset - OFFSET_CHANGE;

      searchParams.set("offset", newOffset.toString());
    } else {
      searchParams.delete("offset");
    }
    setSearchParams(searchParams);
  };

  const onDecrement = () => {
    const newOffset = offset + OFFSET_CHANGE;

    searchParams.set("offset", newOffset.toString());
    setSearchParams(searchParams);
  };

  const { flatNodes } = useMemo(
    () => flattenNodes(structure?.nodes ?? [], openGroupIds),
    [structure?.nodes, openGroupIds],
  );

  return (
    <Flex justifyContent="flex-end" mr={3} position="relative" pt="75px" width="100%">
      <Box position="absolute" top="175px" width="100%">
        <TaskNames nodes={flatNodes} />
      </Box>
      <Box>
        <Flex position="relative">
          <IconButton
            aria-label={`-${OFFSET_CHANGE} older dag runs`}
            disabled={runs.length < limit}
            height="98px"
            minW={0}
            mr={10}
            onClick={onDecrement}
            title={`-${OFFSET_CHANGE} older dag runs`}
            variant="surface"
            zIndex={1}
          >
            <FiChevronLeft />
          </IconButton>
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
            {runs.map((dr, index) => (
              <Bar index={index} key={dr.dag_run_id} limit={limit} max={max} nodes={flatNodes} run={dr} />
            ))}
          </Flex>
          <IconButton
            aria-label={`+${OFFSET_CHANGE} newer dag runs`}
            disabled={offset === 0}
            height="98px"
            minW={0}
            ml={1}
            onClick={onIncrement}
            title={`+${OFFSET_CHANGE} newer dag runs`}
            variant="surface"
            zIndex={1}
          >
            <FiChevronRight />
          </IconButton>
        </Flex>
      </Box>
    </Flex>
  );
};
