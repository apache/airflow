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

import React from "react";
import {
  Tr,
  Td,
  Text,
  Box,
  TextProps,
  TableCellProps,
  Flex,
  BoxProps,
} from "@chakra-ui/react";

import { useGridData } from "src/api";
import { getDuration, formatDuration } from "src/datetime_utils";
import useSelection from "src/dag/useSelection";
import type { DagRun, Task } from "src/types";

import DagRunBar from "./Bar";
import ToggleGroups from "../ToggleGroups";

const DurationAxis = (props: BoxProps) => (
  <Box
    position="absolute"
    borderBottomWidth={1}
    zIndex={0}
    opacity={0.7}
    width="100%"
    {...props}
  />
);

const DurationTick = ({ children, ...rest }: TextProps) => (
  <Text
    fontSize="sm"
    color="gray.400"
    right={1}
    position="absolute"
    whiteSpace="nowrap"
    {...rest}
  >
    {children}
  </Text>
);

const Th = (props: TableCellProps) => (
  <Td
    position="sticky"
    top={0}
    zIndex={1}
    p={0}
    height="155px"
    bg="white"
    {...props}
  />
);

export interface RunWithDuration extends DagRun {
  duration: number;
}

interface Props {
  groups?: Task;
  openGroupIds?: string[];
  onToggleGroups?: (groupIds: string[]) => void;
  isGridCollapsed?: boolean;
}

const DagRuns = ({
  groups,
  openGroupIds,
  onToggleGroups,
  isGridCollapsed,
}: Props) => {
  const {
    data: { dagRuns },
  } = useGridData();
  const { selected, onSelect } = useSelection();
  const durations: number[] = [];
  const runs: RunWithDuration[] = dagRuns
    .map((dagRun) => {
      const duration = getDuration(dagRun.startDate, dagRun.endDate);
      durations.push(duration);
      return {
        ...dagRun,
        duration,
      };
    })
    .filter((dr, i) => {
      if (isGridCollapsed) {
        if (selected.runId) return dr.runId === selected.runId;
        return i === dagRuns.length - 1;
      }
      return true;
    });

  // calculate dag run bar heights relative to max
  const max = Math.max.apply(null, durations);

  return (
    <Tr>
      {!isGridCollapsed && (
        <Th left={0} zIndex={2}>
          <Flex
            borderBottomWidth={3}
            position="relative"
            height="100%"
            width="100%"
            flexDirection="column-reverse"
            pb={2}
          >
            {!!runs.length && (
              <>
                {!!(groups && openGroupIds && onToggleGroups) && (
                  <ToggleGroups
                    groups={groups}
                    openGroupIds={openGroupIds}
                    onToggleGroups={onToggleGroups}
                  />
                )}
                <DurationTick bottom="120px">Duration</DurationTick>
                <DurationTick bottom="96px">{formatDuration(max)}</DurationTick>
                <DurationTick bottom="46px">
                  {formatDuration(max / 2)}
                </DurationTick>
                <DurationTick bottom={0}>00:00:00</DurationTick>
              </>
            )}
          </Flex>
        </Th>
      )}
      <Th
        align="right"
        verticalAlign="bottom"
        borderRightWidth="16px"
        borderRightColor="white"
      >
        <Flex
          justifyContent="flex-end"
          borderBottomWidth={3}
          position="relative"
          borderRightWidth="16px"
          borderRightColor="white"
          marginRight="-16px"
          borderTopWidth="50px"
          borderTopColor="white"
        >
          {runs.map((run: RunWithDuration, index) => (
            <DagRunBar
              key={run.runId}
              run={run}
              index={index}
              totalRuns={runs.length}
              max={max}
              isSelected={run.runId === selected.runId && !isGridCollapsed}
              onSelect={onSelect}
            />
          ))}
          <DurationAxis bottom="100px" />
          <DurationAxis bottom="50px" />
          <DurationAxis bottom="4px" />
        </Flex>
      </Th>
    </Tr>
  );
};

export default DagRuns;
