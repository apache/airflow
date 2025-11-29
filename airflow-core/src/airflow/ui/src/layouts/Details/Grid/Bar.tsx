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

import type { GridRunsResponse } from "openapi/requests";
import { RunTypeIcon } from "src/components/RunTypeIcon";
import { BundleVersionIndicator, DagVersionIndicator } from "src/components/ui/VersionIndicator";
import { VersionIndicatorDisplayOptions } from "src/constants/showVersionIndicatorOptions";
import { useGridTiSummaries } from "src/queries/useGridTISummaries.ts";

import { GridButton } from "./GridButton";
import { TaskInstancesColumn } from "./TaskInstancesColumn";
import type { GridTask } from "./utils";

const BAR_HEIGHT = 100;

type Props = {
  readonly bundleVersion?: string;
  readonly dagVersionNumber?: number;
  readonly hasMixedVersions?: boolean;
  readonly isBundleVersionChange?: boolean;
  readonly isDagVersionChange?: boolean;
  readonly max: number;
  readonly nodes: Array<GridTask>;
  readonly onCellClick?: () => void;
  readonly onColumnClick?: () => void;
  readonly run: GridRunsResponse;
  readonly showVersionIndicatorMode?: string;
};

export const Bar = ({
  bundleVersion,
  dagVersionNumber,
  hasMixedVersions,
  isBundleVersionChange,
  isDagVersionChange,
  max,
  nodes,
  onCellClick,
  onColumnClick,
  run,
  showVersionIndicatorMode,
}: Props) => {
  const { dagId = "", runId } = useParams();
  const [searchParams] = useSearchParams();

  const isSelected = runId === run.run_id;
  const search = searchParams.toString();
  const { data: gridTISummaries } = useGridTiSummaries({
    dagId,
    runId: run.run_id,
    state: run.state,
  });

  const taskInstances = gridTISummaries?.task_instances ?? [];

  return (
    <Box
      _hover={{ bg: "brand.subtle" }}
      bg={isSelected ? "brand.muted" : undefined}
      position="relative"
      transition="background-color 0.2s"
    >
      {isBundleVersionChange === true &&
      (showVersionIndicatorMode === VersionIndicatorDisplayOptions.BUNDLE ||
        showVersionIndicatorMode === VersionIndicatorDisplayOptions.ALL) ? (
        <BundleVersionIndicator bundleVersion={bundleVersion} />
      ) : undefined}
      {isDagVersionChange === true &&
      (showVersionIndicatorMode === VersionIndicatorDisplayOptions.DAG ||
        showVersionIndicatorMode === VersionIndicatorDisplayOptions.ALL) ? (
        <DagVersionIndicator dagVersionNumber={dagVersionNumber} orientation="vertical" />
      ) : undefined}

      <Flex
        alignItems="flex-end"
        height={BAR_HEIGHT}
        justifyContent="center"
        onClick={onColumnClick}
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

      <TaskInstancesColumn
        hasMixedVersions={hasMixedVersions}
        nodes={nodes}
        onCellClick={onCellClick}
        runId={run.run_id}
        showVersionIndicatorMode={showVersionIndicatorMode}
        taskInstances={taskInstances}
      />
    </Box>
  );
};
