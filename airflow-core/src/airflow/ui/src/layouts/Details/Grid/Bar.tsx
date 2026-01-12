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
import { Flex, Box } from "@chakra-ui/react";
import { useParams, useSearchParams } from "react-router-dom";

import { RunTypeIcon } from "src/components/RunTypeIcon";
import { BundleVersionIndicator, DagVersionIndicator } from "src/components/ui/VersionIndicator";
import type { VersionIndicatorDisplayOption } from "src/constants/showVersionIndicatorOptions";
import { VersionIndicatorDisplayOptions } from "src/constants/showVersionIndicatorOptions";
import { useHover } from "src/context/hover";

import { GridButton } from "./GridButton";
import type { GridRunWithVersionFlags } from "./useGridRunsWithVersionFlags";

const BAR_HEIGHT = 100;

type Props = {
  readonly max: number;
  readonly onClick?: () => void;
  readonly run: GridRunWithVersionFlags;
  readonly showVersionIndicatorMode?: VersionIndicatorDisplayOption;
};

export const Bar = ({ max, onClick, run, showVersionIndicatorMode }: Props) => {
  const { dagId = "", runId } = useParams();
  const [searchParams] = useSearchParams();
  const { hoveredRunId, setHoveredRunId } = useHover();

  const isSelected = runId === run.run_id;
  const isHovered = hoveredRunId === run.run_id;
  const search = searchParams.toString();

  const handleMouseEnter = () => setHoveredRunId(run.run_id);
  const handleMouseLeave = () => setHoveredRunId(undefined);

  return (
    <Box
      bg={isSelected ? "brand.emphasized" : isHovered ? "brand.muted" : undefined}
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
      position="relative"
      transition="background-color 0.2s"
    >
      {run.isBundleVersionChange &&
      (showVersionIndicatorMode === VersionIndicatorDisplayOptions.BUNDLE ||
        showVersionIndicatorMode === VersionIndicatorDisplayOptions.ALL) ? (
        <BundleVersionIndicator bundleVersion={run.bundle_version ?? undefined} />
      ) : undefined}
      {run.isDagVersionChange &&
      (showVersionIndicatorMode === VersionIndicatorDisplayOptions.DAG ||
        showVersionIndicatorMode === VersionIndicatorDisplayOptions.ALL) ? (
        <DagVersionIndicator dagVersionNumber={run.dag_version_number ?? undefined} orientation="vertical" />
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
