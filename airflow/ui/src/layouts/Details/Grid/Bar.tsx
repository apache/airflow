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
import { Flex, Box, VStack, Text } from "@chakra-ui/react";
import { useParams, useSearchParams } from "react-router-dom";

import { RunTypeIcon } from "src/components/RunTypeIcon";
import Time from "src/components/Time";

import { GridButton } from "./GridButton";
import { TaskInstances } from "./TaskInstances";
import type { GridTask, RunWithDuration } from "./utils";

const BAR_HEIGHT = 100;

type Props = {
  readonly index: number;
  readonly limit: number;
  readonly max: number;
  readonly nodes: Array<GridTask>;
  readonly run: RunWithDuration;
};

export const Bar = ({ index, limit, max, nodes, run }: Props) => {
  const { dagId = "", runId } = useParams();
  const [searchParams] = useSearchParams();

  const isSelected = runId === run.dag_run_id;

  const search = searchParams.toString();

  const shouldShowTick = index % 8 === 0 || index === limit - 1;

  return (
    <Box
      _hover={{ bg: "blue.subtle" }}
      bg={isSelected ? "blue.muted" : undefined}
      position="relative"
      transition="background-color 0.2s"
    >
      <Flex
        alignItems="flex-end"
        height={BAR_HEIGHT}
        justifyContent="center"
        pb="2px"
        px="5px"
        width="14px"
        zIndex={1}
      >
        <GridButton
          alignItems="center"
          dagId={dagId}
          flexDir="column"
          height={`${(run.duration / max) * BAR_HEIGHT}px`}
          justifyContent="flex-end"
          label={run.run_after}
          minHeight="14px"
          runId={run.dag_run_id}
          searchParams={search}
          state={run.state}
          zIndex={1}
        >
          {run.run_type !== "scheduled" && <RunTypeIcon runType={run.run_type} size="10px" />}
        </GridButton>
        {shouldShowTick ? (
          <VStack gap={0} left="8px" position="absolute" top={0} width={0} zIndex={-1}>
            <Text
              color="border.emphasized"
              fontSize="xs"
              mt="-35px !important"
              transform="rotate(-40deg) translateX(28px)"
              whiteSpace="nowrap"
            >
              <Time datetime={run.run_after} format="MMM DD, HH:mm" />
            </Text>
            <Box borderLeftWidth={1} height="100px" opacity={0.7} zIndex={0} />
          </VStack>
        ) : undefined}
      </Flex>
      <TaskInstances nodes={nodes} runId={run.dag_run_id} taskInstances={run.task_instances} />
    </Box>
  );
};
