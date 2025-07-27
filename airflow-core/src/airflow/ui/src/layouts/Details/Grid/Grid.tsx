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
import { Box, Flex, IconButton, Text } from "@chakra-ui/react";
import dayjs from "dayjs";
import dayjsDuration from "dayjs/plugin/duration";
import { useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { FiChevronsRight } from "react-icons/fi";
import { Link, useParams } from "react-router-dom";

import type { GridRunsResponse } from "openapi/requests";
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
  readonly limit: number;
};

export const Grid = ({ limit }: Props) => {
  const { t: translate } = useTranslation("dag");
  const gridRef = useRef<HTMLDivElement>(null);
  const [isGridFocused, setIsGridFocused] = useState(false);

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

  const { setMode } = useNavigation({
    enabled: isGridFocused,
    runs: gridRuns ?? [],
    tasks: flatNodes,
  });

  useEffect(() => {
    if (gridRef.current && gridRuns && flatNodes.length > 0) {
      gridRef.current.focus();
      setIsGridFocused(true);
    }
  }, [gridRuns, flatNodes.length]);

  const handleFocus = () => {
    setIsGridFocused(true);
  };

  const handleBlur = (event: React.FocusEvent) => {
    if (!gridRef.current?.contains(event.relatedTarget as Node)) {
      setIsGridFocused(false);
    }
  };

  const handleMouseDown = (event: React.MouseEvent) => {
    event.preventDefault();
    if (gridRef.current) {
      gridRef.current.focus();
      setIsGridFocused(true);
    }
  };

  return (
    <Flex
      _focus={{
        borderRadius: "4px",
        boxShadow: "0 0 0 2px rgba(59, 130, 246, 0.5)",
      }}
      cursor="pointer"
      justifyContent="flex-start"
      onBlur={handleBlur}
      onFocus={handleFocus}
      onMouseDown={handleMouseDown}
      outline="none"
      position="relative"
      pt={50}
      ref={gridRef}
      tabIndex={0}
      width="100%"
    >
      {Boolean(isGridFocused) && (
        <Box
          borderRadius="md"
          color="gray.400"
          fontSize="xs"
          position="absolute"
          px={2}
          py={10}
          top={0}
          zIndex={10}
        >
          <Text>{translate("navigation.navigation", { arrow: "↑↓←→" })}</Text>
          <Text>{translate("navigation.longPress", { arrow: "↑↓←→" })}</Text>
          <Text>{translate("navigation.jump", { arrow: "↑↓←→" })}</Text>
        </Box>
      )}

      <Box flexGrow={1} minWidth={7} position="relative" top="100px">
        <TaskNames nodes={flatNodes} onRowClick={() => setMode("row")} />
      </Box>
      <Box position="relative">
        <Flex position="relative">
          <DurationAxis top="100px" />
          <DurationAxis top="50px" />
          <DurationAxis top="4px" />
          <Flex flexDirection="column-reverse" height="100px" position="relative" width="100%">
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
                onCellClick={() => setMode("grid")}
                onColumnClick={() => setMode("column")}
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
