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
import { Flex, Box, Center, Button, Icon } from "@chakra-ui/react";
import { LuClock, LuTriangleAlert } from "react-icons/lu";
import { useParams, useSearchParams, useNavigate } from "react-router-dom";

import { RunTypeIcon } from "src/components/RunTypeIcon";
import { VersionIndicatorOptions } from "src/constants/showVersionIndicatorOptions";
import { useHover } from "src/context/hover";

import { GridButton } from "./GridButton";
import { BundleVersionIndicator, DagVersionIndicator } from "./VersionIndicator";
import { BAR_HEIGHT } from "./constants";
import {
  getBundleVersion,
  getMaxVersionNumber,
  type GridRunWithVersionFlags,
} from "./useGridRunsWithVersionFlags";

const ICON_GAP_PX = 4;
const ICON_HEIGHT_PX = 16;
const BAR_PADDING_BOTTOM_PX = 2;
const BAR_MIN_HEIGHT_PX = 14;

type Props = {
  readonly max: number;
  readonly onClick?: () => void;
  readonly run: GridRunWithVersionFlags;
  readonly showVersionIndicatorMode?: VersionIndicatorOptions;
};

export const Bar = ({ max, onClick, run, showVersionIndicatorMode }: Props) => {
  const { dagId = "", runId } = useParams();
  const [searchParams] = useSearchParams();
  const { hoveredRunId, setHoveredRunId } = useHover();

  const isSelected = runId === run.run_id;
  const isHovered = hoveredRunId === run.run_id;
  const search = searchParams.toString();
  const isFailed = (run.state ?? "").toLowerCase() === "failed";
  const hasMissedDeadline = Boolean(run.has_missed_deadline);
  const barHeightPx = max > 0 ? (run.duration / max) * BAR_HEIGHT : 0;

  const handleMouseEnter = () => setHoveredRunId(run.run_id);
  const handleMouseLeave = () => setHoveredRunId(undefined);

  const navigate = useNavigate();

  const handleDeadlineIconClick = () => {
    void navigate({ pathname: `/dags/${dagId}/runs/${run.run_id}/deadlines`, search });
  };

  // Account for minHeight and padding-bottom so icons always appear above the rendered bar
  const effectiveBarHeightPx = Math.max(barHeightPx, BAR_MIN_HEIGHT_PX) + BAR_PADDING_BOTTOM_PX;
  const failedIconBottom = effectiveBarHeightPx + ICON_GAP_PX;
  const deadlineIconBottom = isFailed ? failedIconBottom + ICON_HEIGHT_PX : failedIconBottom;

  return (
    <Box
      bg={isSelected ? "brand.emphasized" : isHovered ? "brand.muted" : undefined}
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
      position="relative"
      transition="background-color 0.2s"
    >
      {hasMissedDeadline ? (
        <Center bottom={`${deadlineIconBottom}px`} left={0} position="absolute" right={0} zIndex={2}>
          <Button
            _focusVisible={{ boxShadow: "none" }}
            borderRadius={0}
            h="auto"
            lineHeight={1}
            m={0}
            minH={0}
            minW={0}
            onClick={handleDeadlineIconClick}
            p={0}
            variant="ghost"
          >
            <Icon as={LuClock} boxSize={3} color="warning.solid" />
          </Button>
        </Center>
      ) : undefined}
      {isFailed ? (
        <Center bottom={`${failedIconBottom}px`} left={0} position="absolute" right={0} zIndex={2}>
          <Icon as={LuTriangleAlert} boxSize={3} color="failed.solid" />
        </Center>
      ) : undefined}
      {run.isBundleVersionChange &&
      (showVersionIndicatorMode === VersionIndicatorOptions.BUNDLE_VERSION ||
        showVersionIndicatorMode === VersionIndicatorOptions.ALL) ? (
        <BundleVersionIndicator bundleVersion={getBundleVersion(run)} />
      ) : undefined}
      {run.isDagVersionChange &&
      (showVersionIndicatorMode === VersionIndicatorOptions.DAG_VERSION ||
        showVersionIndicatorMode === VersionIndicatorOptions.ALL) ? (
        <DagVersionIndicator dagVersionNumber={getMaxVersionNumber(run)} orientation="vertical" />
      ) : undefined}

      <Flex
        alignItems="flex-end"
        height={BAR_HEIGHT}
        justifyContent="center"
        onClick={onClick}
        pb="2px"
        px="5px"
        width="18px"
        zIndex={1}
      >
        <GridButton
          alignItems="center"
          color="fg"
          dagId={dagId}
          flexDir="column"
          height={`${(run.duration / max) * BAR_HEIGHT}px`}
          justifyContent="flex-end"
          label={run.run_after}
          minHeight="14px"
          runId={run.run_id}
          searchParams={search}
          state={run.state}
          zIndex={1}
        >
          {run.run_type !== "scheduled" && <RunTypeIcon color="white" runType={run.run_type} size="10px" />}
        </GridButton>
      </Flex>
    </Box>
  );
};
