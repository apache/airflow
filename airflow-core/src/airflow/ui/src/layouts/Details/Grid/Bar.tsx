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
import { Flex, Box, Text } from "@chakra-ui/react";
import { useParams, useSearchParams } from "react-router-dom";

import type { GridRunsResponse } from "openapi/requests";
import { RunTypeIcon } from "src/components/RunTypeIcon";
import { useGridTiSummaries } from "src/queries/useGridTISummaries.ts";

import { GridButton } from "./GridButton";
import { TaskInstancesColumn } from "./TaskInstancesColumn";
import type { GridTask } from "./utils";

const BAR_HEIGHT = 100;

type VersionIndicatorProps = {
  readonly ariaLabel: string;
  readonly type: "dagrun" | "mixed";
  readonly versionNumber?: number | null;
};

const VersionIndicator = ({ ariaLabel, type, versionNumber }: VersionIndicatorProps) => {
  const isDagRun = type === "dagrun";

  return (
    <Box
      aria-label={ariaLabel}
      bg="orange.400"
      height="100px"
      left={isDagRun ? "-1px" : undefined}
      position="absolute"
      right={isDagRun ? undefined : "-1px"}
      top="0"
      width="2px"
      zIndex={isDagRun ? 3 : 4}
    >
      <Text
        aria-label={`Version ${versionNumber ?? "unknown"}`}
        color="orange.700"
        fontSize="10px"
        fontWeight="bold"
        left={isDagRun ? "-5px" : undefined}
        position="absolute"
        right={isDagRun ? undefined : "-5px"}
        top="-16px"
        whiteSpace="nowrap"
      >
        {`v${versionNumber ?? ""}`}
      </Text>
    </Box>
  );
};

type Props = {
  readonly hasMixedVersions?: boolean | null;
  readonly latestVersionNumber?: number | null;
  readonly max: number;
  readonly nodes: Array<GridTask>;
  readonly run: GridRunsResponse;
  readonly showVersionIndicator?: boolean;
  readonly versionNumber?: number | null;
};

export const Bar = ({
  hasMixedVersions = false,
  latestVersionNumber,
  max,
  nodes,
  run,
  showVersionIndicator = false,
  versionNumber,
}: Props) => {
  const { dagId = "", runId } = useParams();
  const [searchParams] = useSearchParams();

  const isSelected = runId === run.run_id;

  const search = searchParams.toString();
  const { data: gridTISummaries } = useGridTiSummaries({ dagId, runId: run.run_id, state: run.state });

  return (
    <Box
      _hover={{ bg: "blue.subtle" }}
      bg={isSelected ? "blue.muted" : undefined}
      position="relative"
      transition="background-color 0.2s"
    >
      {Boolean(showVersionIndicator) && (
        <VersionIndicator
          ariaLabel="DAG version change indicator"
          type="dagrun"
          versionNumber={versionNumber}
        />
      )}

      {Boolean(hasMixedVersions) && (
        <VersionIndicator
          ariaLabel="Mixed version indicator"
          type="mixed"
          versionNumber={latestVersionNumber}
        />
      )}

      <Flex
        alignItems="flex-end"
        height={BAR_HEIGHT}
        justifyContent="center"
        pb="2px"
        px="5px"
        width="18px"
        zIndex={1}
      >
        <GridButton
          alignItems="center"
          color="white"
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
          {run.run_type !== "scheduled" && <RunTypeIcon runType={run.run_type} size="10px" />}
        </GridButton>
      </Flex>
      <TaskInstancesColumn
        nodes={nodes}
        runId={run.run_id}
        taskInstances={gridTISummaries?.task_instances ?? []}
      />
    </Box>
  );
};
