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
import { Box, Flex, Heading, HStack } from "@chakra-ui/react";
import { FiClipboard } from "react-icons/fi";

import { useMonitorServiceGetHealth } from "openapi/queries";
import { ErrorAlert } from "src/components/ErrorAlert";

import { HealthTag } from "./HealthTag";

export const Health = () => {
  const { data, error, isLoading } = useMonitorServiceGetHealth();

  return (
    <Box>
      <Flex color="gray.500" mb={2}>
        <FiClipboard />
        <Heading ml={1} size="xs">
          Health
        </Heading>
      </Flex>
      <ErrorAlert error={error} />
      <HStack alignItems="center" spacing={2}>
        <HealthTag
          isLoading={isLoading}
          status={data?.metadatabase.status}
          title="MetaDatabase"
        />
        <HealthTag
          isLoading={isLoading}
          latestHeartbeat={data?.scheduler.latest_scheduler_heartbeat}
          status={data?.scheduler.status}
          title="Scheduler"
        />
        <HealthTag
          isLoading={isLoading}
          latestHeartbeat={data?.triggerer.latest_triggerer_heartbeat}
          status={data?.triggerer.status}
          title="Triggerer"
        />
        {/* TODO add is_standalone to API to determine when to render this */}
        <HealthTag
          isLoading={isLoading}
          latestHeartbeat={data?.dag_processor.latest_dag_processor_heartbeat}
          status={data?.dag_processor.status}
          title="Dag Processor"
        />
      </HStack>
    </Box>
  );
};
