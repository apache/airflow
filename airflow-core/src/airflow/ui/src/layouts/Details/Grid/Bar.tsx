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
import { VersionIndicatorOptions } from "src/constants/showVersionIndicatorOptions";

import { GridButton } from "./GridButton";
import { BundleVersionIndicator, DagVersionIndicator } from "./VersionIndicator";
import { BAR_HEIGHT, NOTE_GRADIENT } from "./constants";
import {
  getBundleVersion,
  getMaxVersionNumber,
  type GridRunWithVersionFlags,
} from "./useGridRunsWithVersionFlags";

type Props = {
  readonly max: number;
  readonly onClick?: () => void;
  readonly run: GridRunWithVersionFlags;
  readonly showVersionIndicatorMode?: VersionIndicatorOptions;
};

export const Bar = ({ max, onClick, run, showVersionIndicatorMode }: Props) => {
  const { dagId = "", runId } = useParams();
  const [searchParams] = useSearchParams();

  const isSelected = runId === run.run_id;
  const search = searchParams.toString();

  return (
    <Box
      bg={isSelected ? "brand.emphasized" : undefined}
      data-run-id={run.run_id}
      data-selected={isSelected}
      position="relative"
      transition="background-color 0.2s"
    >
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
          colorPalette={run.state ?? "none"}
          dagId={dagId}
          duration={run.duration}
          flexDir="column"
          height={`${(run.duration / max) * BAR_HEIGHT}px`}
          justifyContent="flex-end"
          label={run.run_after}
          minHeight="14px"
          runId={run.run_id}
          searchParams={search}
          state={run.state}
          style={run.has_note ? { background: NOTE_GRADIENT } : undefined}
          zIndex={1}
        >
          {run.run_type !== "scheduled" && <RunTypeIcon color="white" runType={run.run_type} size="10px" />}
        </GridButton>
      </Flex>
    </Box>
  );
};
