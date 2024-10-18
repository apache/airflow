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
import { Card, CardBody, CardHeader, Flex, Heading } from "@chakra-ui/react";
import InfoTooltip from "src/components/InfoTooltip";
import FilterBar from "src/cluster-activity/nav/FilterBar";
import useFilters from "src/cluster-activity/useFilters";
import { useHistoricalMetricsData } from "src/api";
import PieChart from "src/cluster-activity/historical-metrics/PieChart";
import LoadingWrapper from "src/components/LoadingWrapper";

const HistoricalMetrics = () => {
  const {
    filters: { startDate, endDate },
  } = useFilters();
  const { data, isError } = useHistoricalMetricsData(startDate, endDate);
  return (
    <Flex w="100%">
      <Card w="100%">
        <CardHeader>
          <Flex alignItems="center">
            <Heading size="md">Historical metrics</Heading>
            <InfoTooltip
              label="Based on historical data. You can adjust the period by setting a different start and end date filter."
              size={18}
            />
          </Flex>
        </CardHeader>
        <CardBody>
          <FilterBar />
          <Flex justifyContent="center" minH="200px" alignItems="center">
            <LoadingWrapper hasData={!!data} isError={isError}>
              <Flex flexWrap="wrap" width="100%">
                <PieChart
                  title="Dag Run States"
                  data={data?.dagRunStates}
                  width="33.33%"
                  minW="300px"
                  minH="350px"
                  pr={4}
                  colorPalette={{
                    failed: stateColors.failed,
                    queued: stateColors.queued,
                    running: stateColors.running,
                    success: stateColors.success,
                  }}
                />
                <PieChart
                  title="Dag Run Types"
                  data={data?.dagRunTypes}
                  width="33.33%"
                  minW="300px"
                  minH="350px"
                  pr={4}
                  colorPalette={{
                    backfill: stateColors.deferred,
                    assetTriggered: stateColors.queued,
                    manual: stateColors.success,
                    scheduled: stateColors.scheduled,
                  }}
                />
                <PieChart
                  title="Task Instance States"
                  data={data?.taskInstanceStates}
                  width="33.33%"
                  minW="300px"
                  minH="350px"
                />
              </Flex>
            </LoadingWrapper>
          </Flex>
        </CardBody>
      </Card>
    </Flex>
  );
};

export default HistoricalMetrics;
