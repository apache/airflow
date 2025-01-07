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
import { Box, Flex, Heading, HStack, SimpleGrid, Text } from "@chakra-ui/react";
import { FiCalendar } from "react-icons/fi";

import type { DAGDetailsResponse, DAGRunResponse } from "openapi/requests/types.gen";
import { DagIcon } from "src/assets/DagIcon";
import DagRunInfo from "src/components/DagRunInfo";
import DocumentationModal from "src/components/DocumentationModal";
import ParseDag from "src/components/ParseDag";
import { Stat } from "src/components/Stat";
import { TogglePause } from "src/components/TogglePause";
import TriggerDAGButton from "src/components/TriggerDag/TriggerDAGButton";
import { Tooltip } from "src/components/ui";

import { DagTags } from "../DagsList/DagTags";

export const Header = ({
  dag,
  dagId,
  latestRun,
}: {
  readonly dag?: DAGDetailsResponse;
  readonly dagId?: string;
  readonly latestRun?: DAGRunResponse;
}) => (
  <Box borderColor="border" borderRadius={8} borderWidth={1} p={2}>
    <Box p={2}>
      <Flex alignItems="center" justifyContent="space-between">
        <HStack alignItems="center" gap={2}>
          <DagIcon height={8} width={8} />
          <Heading size="lg">{dag?.dag_display_name ?? dagId}</Heading>
          {dag !== undefined && (
            <TogglePause dagDisplayName={dag.dag_display_name} dagId={dag.dag_id} isPaused={dag.is_paused} />
          )}
        </HStack>
        <Flex>
          {dag ? (
            <HStack>
              {dag.doc_md === null ? undefined : <DocumentationModal docMd={dag.doc_md} docType="Dag" />}
              <ParseDag dagId={dag.dag_id} fileToken={dag.file_token} />
              <TriggerDAGButton dag={dag} />
            </HStack>
          ) : undefined}
        </Flex>
      </Flex>
      <SimpleGrid columns={4} gap={4} my={2}>
        <Stat label="Schedule">
          {Boolean(dag?.timetable_summary) ? (
            <Tooltip content={dag?.timetable_description} showArrow>
              <Text fontSize="sm">
                <FiCalendar style={{ display: "inline" }} /> {dag?.timetable_summary}
              </Text>
            </Tooltip>
          ) : undefined}
        </Stat>
        <Stat label="Last Run">
          {Boolean(latestRun) && latestRun !== undefined ? (
            <DagRunInfo
              dataIntervalEnd={latestRun.data_interval_end}
              dataIntervalStart={latestRun.data_interval_start}
              endDate={latestRun.end_date}
              startDate={latestRun.start_date}
              state={latestRun.state}
            />
          ) : undefined}
        </Stat>
        <Stat label="Next Run">
          {Boolean(dag?.next_dagrun) && dag !== undefined ? (
            <DagRunInfo
              dataIntervalEnd={dag.next_dagrun_data_interval_end}
              dataIntervalStart={dag.next_dagrun_data_interval_start}
              nextDagrunCreateAfter={dag.next_dagrun_create_after}
            />
          ) : undefined}
        </Stat>
        <div />
        <div />
      </SimpleGrid>
    </Box>
    <Flex
      alignItems="center"
      bg="bg.muted"
      borderTopColor="border"
      borderTopWidth={1}
      color="fg.subtle"
      fontSize="sm"
      justifyContent="space-between"
      px={2}
      py={1}
    >
      <Text>Owner: {dag?.owners.join(", ")}</Text>
      <DagTags tags={dag?.tags ?? []} />
    </Flex>
  </Box>
);
