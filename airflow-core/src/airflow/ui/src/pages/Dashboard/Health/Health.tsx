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
import { useTranslation } from "react-i18next";
import { MdOutlineHealthAndSafety } from "react-icons/md";

import { useMonitorServiceGetHealth } from "openapi/queries";
import type {
  DagProcessorInstanceInfoResponse,
  SchedulerInstanceInfoResponse,
  TriggererInstanceInfoResponse,
} from "openapi/requests/types.gen";

import { ErrorAlert } from "src/components/ErrorAlert";

import { useAutoRefresh } from "src/utils";

import { HealthBadge } from "./HealthBadge";
import type { HealthInstance } from "./HealthInstances";

const schedulerInstances = (instances?: Array<SchedulerInstanceInfoResponse> | null) =>
  instances?.map((instance): HealthInstance => ({
    hostname: instance.hostname,
    latestHeartbeat: instance.latest_scheduler_heartbeat,
  }));

const triggererInstances = (instances?: Array<TriggererInstanceInfoResponse> | null) =>
  instances?.map((instance): HealthInstance => ({
    hostname: instance.hostname,
    latestHeartbeat: instance.latest_triggerer_heartbeat,
    teamName: instance.team_name,
  }));

const dagProcessorInstances = (instances?: Array<DagProcessorInstanceInfoResponse> | null) =>
  instances?.map((instance): HealthInstance => ({
    bundleNames: instance.bundle_names,
    hostname: instance.hostname,
    latestHeartbeat: instance.latest_dag_processor_heartbeat,
  }));

export const Health = () => {
  const refetchInterval = useAutoRefresh({ checkPendingRuns: true });

  const { data, error, isLoading } = useMonitorServiceGetHealth(undefined, {
    refetchInterval,
  });
  const { t: translate } = useTranslation("dashboard");

  return (
    <Box>
      <Flex color="fg.muted" mb={2}>
        <MdOutlineHealthAndSafety />
        <Heading ml={1} size="xs">
          {translate("health.health")}
        </Heading>
      </Flex>
      <ErrorAlert error={error} />
      <HStack alignItems="center" flexWrap={{ base: "wrap", md: "nowrap" }} gap={2}>
        <HealthBadge
          isLoading={isLoading}
          status={data?.metadatabase.status}
          title={translate("health.metaDatabase")}
        />
        {/* ``detailed_status`` is preferred over the legacy ``status``: the latter only reports
            whether one replica is alive, while the former also reports "degraded" when the component
            divides its work up and part of that work has no live replica covering it. Which work
            that is differs per component, so each passes its own explanation of "degraded". */}
        <HealthBadge
          instances={schedulerInstances(data?.scheduler.instances)}
          isLoading={isLoading}
          latestHeartbeat={data?.scheduler.latest_scheduler_heartbeat}
          status={data?.scheduler.detailed_status ?? data?.scheduler.status}
          title={translate("health.scheduler")}
        />
        <HealthBadge
          degradedHint={translate("health.degradedHint.triggerer")}
          instances={triggererInstances(data?.triggerer.instances)}
          isLoading={isLoading}
          latestHeartbeat={data?.triggerer.latest_triggerer_heartbeat}
          status={data?.triggerer.detailed_status ?? data?.triggerer.status}
          title={translate("health.triggerer")}
        />
        {data?.dag_processor ? (
          <HealthBadge
            degradedHint={translate("health.degradedHint.dagProcessor")}
            instances={dagProcessorInstances(data.dag_processor.instances)}
            isLoading={isLoading}
            latestHeartbeat={data.dag_processor.latest_dag_processor_heartbeat}
            status={data.dag_processor.detailed_status ?? data.dag_processor.status}
            title={translate("health.dagProcessor")}
          />
        ) : undefined}
      </HStack>
    </Box>
  );
};
