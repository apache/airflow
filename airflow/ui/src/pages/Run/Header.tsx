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
import { Box, Flex, Heading, HStack, SimpleGrid, Spinner, Text } from "@chakra-ui/react";
import { FiBarChart, FiMessageSquare } from "react-icons/fi";

import type { DAGRunResponse } from "openapi/requests/types.gen";
import { ClearRunButton } from "src/components/Clear";
import DisplayMarkdownButton from "src/components/DisplayMarkdownButton";
import { MarkRunAsButton } from "src/components/MarkAs";
import { RunTypeIcon } from "src/components/RunTypeIcon";
import { Stat } from "src/components/Stat";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { getDuration } from "src/utils";

export const Header = ({
  dagRun,
  isRefreshing,
}: {
  readonly dagRun: DAGRunResponse;
  readonly isRefreshing?: boolean;
}) => (
  <Box borderColor="border" borderRadius={8} borderWidth={1} p={2}>
    <Flex alignItems="center" justifyContent="space-between" mb={2}>
      <HStack alignItems="center" gap={2}>
        <FiBarChart size="1.75rem" />
        <Heading size="lg">
          <strong>Run: </strong>
          {dagRun.dag_run_id}
        </Heading>
        <StateBadge state={dagRun.state}>{dagRun.state}</StateBadge>
        {isRefreshing ? <Spinner /> : <div />}
      </HStack>
      <HStack>
        {dagRun.note === null || dagRun.note.length === 0 ? undefined : (
          <DisplayMarkdownButton
            header="Dag Run Note"
            icon={<FiMessageSquare color="black" />}
            mdContent={dagRun.note}
            text="Note"
          />
        )}
        <ClearRunButton dagRun={dagRun} />
        <MarkRunAsButton dagRun={dagRun} />
      </HStack>
    </Flex>
    <SimpleGrid columns={4} gap={4}>
      <Stat label="Run Type">
        <HStack>
          <RunTypeIcon runType={dagRun.run_type} />
          <Text>{dagRun.run_type}</Text>
        </HStack>
      </Stat>
      <Stat label="Start">
        <Time datetime={dagRun.start_date} />
      </Stat>
      <Stat label="End">
        <Time datetime={dagRun.end_date} />
      </Stat>
      <Stat label="Duration">{getDuration(dagRun.start_date, dagRun.end_date)}s</Stat>
    </SimpleGrid>
  </Box>
);
