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
  Card,
  CardBody,
  CardHeader,
  Heading,
  Spinner,
} from "@chakra-ui/react";
import { usePools } from "src/api";
import ReactECharts, { ReactEChartsProps } from "src/components/ReactECharts";

const option: ReactEChartsProps["option"] = {
  dataset: {
    source: [
      ["Pool", "Occupied", "Open", "Queued", "Running", "Scheduled"],
      ["Pool 1", 4, 1, 2, 3, 4],
      ["Pool 2", 2, 2, 2, 2, 4],
      ["Pool 3", 2, 2, 2, 2, 4],
      ["Pool 4", 2, 2, 2, 2, 4],
      ["Pool 5", 2, 2, 2, 2, 4],
      ["Pool 6", 2, 2, 2, 2, 4],
      ["Pool 7", 2, 2, 2, 2, 4],
      ["Pool 8", 2, 2, 2, 2, 4],
      ["Pool 9", 2, 2, 2, 2, 4],
      ["Pool 10", 2, 2, 2, 2, 4],
      ["Pool 11", 2, 2, 2, 2, 4],
      ["Pool 12", 2, 2, 2, 2, 4],
      ["Standard Pool", 5, 3, 3, 3, 3],
    ],
  },
  tooltip: {
    trigger: "axis",
    axisPointer: {
      type: "shadow",
    },
  },
  legend: {
    data: ["Occupied", "Open", "Queued", "Running", "Scheduled"],
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
      label: {
        show: true,
      },
    },
    {
      type: "bar",
      stack: "total",
      label: {
        show: true,
      },
    },
    {
      type: "bar",
      stack: "total",
      label: {
        show: true,
      },
    },
    {
      type: "bar",
      stack: "total",
      label: {
        show: true,
      },
    },
    {
      type: "bar",
      stack: "total",
      label: {
        show: true,
      },
    },
  ],
};

const Pools = () => {
  const { data, isSuccess } = usePools();

  console.log(data);

  return (
    <Box flexGrow={1}>
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
