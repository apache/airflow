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
import { Box, Flex, Heading, SimpleGrid } from "@chakra-ui/react";
import { FiClipboard } from "react-icons/fi";

import { useDagServiceGetDags } from "openapi/queries";

import { DAGImportErrors } from "./DAGImportErrors";
import { StatsCard } from "./StatsCard";

export const Stats = () => {
  const { data: activeDagsData, isLoading: isActiveDagsLoading } = useDagServiceGetDags({
    paused: false,
  });

  const { data: failedDagsData, isLoading: isFailedDagsLoading } = useDagServiceGetDags({
    lastDagRunState: "failed",
  });

  const { data: stalledDagsData, isLoading: isStalledDagsLoading } = useDagServiceGetDags({
    lastDagRunState: "queued",
  });

  const { data: runningDagsData, isLoading: isRunningDagsLoading } = useDagServiceGetDags({
    lastDagRunState: "running",
  });

  const activeDagsCount = activeDagsData?.total_entries ?? 0;
  const failedDagsCount = failedDagsData?.total_entries ?? 0;
  const stalledDagsCount = stalledDagsData?.total_entries ?? 0;
  const runningDagsCount = runningDagsData?.total_entries ?? 0;

  return (
    <Box>
      <Flex alignItems="center" color="fg.muted" my={2}>
        <FiClipboard />
        <Heading ml={1} size="xs">
          Stats
        </Heading>
      </Flex>

      <SimpleGrid columns={{ base: 1, lg: 5, md: 3 }} gap={4}>
        <StatsCard
          colorScheme="red"
          count={failedDagsCount}
          isLoading={isFailedDagsLoading}
          label="Failed DAGs"
          link="dags?last_dag_run_state=failed"
        />

        {failedDagsCount > 0 && <DAGImportErrors />}

        {stalledDagsCount > 0 && (
          <StatsCard
            colorScheme="orange"
            count={stalledDagsCount}
            isLoading={isStalledDagsLoading}
            label="Stalled DAGs"
            link="dags?last_dag_run_state=queued"
          />
        )}

        <StatsCard
          colorScheme="teal"
          count={runningDagsCount}
          isLoading={isRunningDagsLoading}
          label="Running DAGs"
          link="dags?last_dag_run_state=running"
        />

        <StatsCard
          colorScheme="blue"
          count={activeDagsCount}
          isLoading={isActiveDagsLoading}
          label="Active DAGS"
          link="dags?paused=false"
        />
      </SimpleGrid>
    </Box>
  );
};
