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
  Heading,
  Spinner,
  useTheme,
} from "@chakra-ui/react";
import { usePools } from "src/api";
import ReactECharts, { ReactEChartsProps } from "src/components/ReactECharts";
import type { API } from "src/types";

const formatData = (data?: API.PoolCollection): Array<Array<any>> =>
  data?.pools?.map((pool) => [
    pool.name,
    pool.openSlots,
    pool.queuedSlots,
    pool.runningSlots,
    pool.scheduledSlots,
  ]) || [];

const Pools = (props: BoxProps) => {
  const { data, isSuccess } = usePools();
  const theme = useTheme();

  const option: ReactEChartsProps["option"] = {
    dataset: {
      source: [
        ["Pool", "Open", "Queued", "Running", "Scheduled"],
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
      data: ["Open", "Queued", "Running", "Scheduled"],
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
          color: "green",
        },
      },
      {
        type: "bar",
        stack: "total",
        barMaxWidth: 10,
        itemStyle: {
          color: theme.colors.gray["600"],
        },
      },
      {
        type: "bar",
        stack: "total",
        barMaxWidth: 10,
        itemStyle: {
          color: theme.colors.blue["200"],
        },
      },
      {
        type: "bar",
        stack: "total",
        barMaxWidth: 10,
        itemStyle: {
          color: theme.colors.yellow["700"],
        },
      },
    ],
  };

  return (
    <Box {...props}>
      {isSuccess ? (
        <Card>
          <CardHeader textAlign="center" p={3}>
            <Heading size="md">Pools Slots</Heading>
          </CardHeader>
          <CardBody>
            <Box height="250px">
              <ReactECharts option={option} />
            </Box>
          </CardBody>
        </Card>
      ) : (
        <Spinner color="blue.500" speed="1s" mr="4px" size="xl" />
      )}
    </Box>
  );
};

export default Pools;
