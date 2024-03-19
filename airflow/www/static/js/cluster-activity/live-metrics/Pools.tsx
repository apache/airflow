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
  Box,
  BoxProps,
  Card,
  CardBody,
  CardHeader,
  Center,
  Heading,
} from "@chakra-ui/react";
import { usePools } from "src/api";
import ReactECharts, { ReactEChartsProps } from "src/components/ReactECharts";
import type { API } from "src/types";
import LoadingWrapper from "src/components/LoadingWrapper";

const formatData = (
  data?: API.PoolCollection
): Array<[string, number, number, number, number, number]> =>
  data?.pools?.map((pool) => [
    pool.name || "",
    pool.openSlots || 0,
    pool.queuedSlots || 0,
    pool.runningSlots || 0,
    pool.scheduledSlots || 0,
    pool.deferredSlots || 0,
  ]) || [];

const Pools = (props: BoxProps) => {
  const { data, isError } = usePools();

  const option: ReactEChartsProps["option"] = {
    dataset: {
      source: [
        ["pool", "open", "queued", "running", "scheduled", "deferred"],
        ...formatData(data),
      ],
    },
    tooltip: {
      trigger: "axis",
      axisPointer: {
        type: "shadow",
      },
    },
    legend: {
      data: ["open", "queued", "running", "scheduled", "deferred"],
    },
    grid: {
      left: "0%",
      right: "5%",
      top: "30%",
      bottom: "0%",
      containLabel: true,
    },
    xAxis: {
      type: "value",
    },
    yAxis: {
      type: "category",
    },
    series: [
      {
        type: "bar",
        stack: "total",
        barMaxWidth: 10,
        itemStyle: {
          color: stateColors.success,
        },
      },
      {
        type: "bar",
        stack: "total",
        barMaxWidth: 10,
        itemStyle: {
          color: stateColors.queued,
        },
      },
      {
        type: "bar",
        stack: "total",
        barMaxWidth: 10,
        itemStyle: {
          color: stateColors.running,
        },
      },
      {
        type: "bar",
        stack: "total",
        barMaxWidth: 10,
        itemStyle: {
          color: stateColors.scheduled,
        },
      },
      {
        type: "bar",
        stack: "total",
        barMaxWidth: 10,
        itemStyle: {
          color: stateColors.deferred,
        },
      },
    ],
  };

  return (
    <Center {...props}>
      <LoadingWrapper hasData={!!data} isError={isError}>
        <Card w="100%">
          <CardHeader textAlign="center" p={3}>
            <Heading size="md">Pools Slots</Heading>
          </CardHeader>
          <CardBody>
            <Box height="250px">
              <ReactECharts option={option} />
            </Box>
          </CardBody>
        </Card>
      </LoadingWrapper>
    </Center>
  );
};

export default Pools;
