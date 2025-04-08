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
import { Box, useToken } from "@chakra-ui/react";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Filler,
  Tooltip,
  type ChartOptions,
} from "chart.js";
import dayjs from "dayjs";
import { useMemo, useRef, useEffect } from "react";
import { Line } from "react-chartjs-2";

import { useColorMode } from "src/context/colorMode";

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Filler, Tooltip);

export type ChartEvent = { timestamp: string | null };

const aggregateEventsIntoIntervals = (events: Array<ChartEvent>, startDate: string, endDate: string) => {
  const totalMinutes = dayjs(endDate).diff(startDate, "minutes");
  const intervalSize = Math.floor(totalMinutes / 10);
  const intervals = Array.from({ length: 10 }).fill(0) as Array<number>;

  events.forEach((event) => {
    if (event.timestamp === null) {
      return;
    }
    const minutesSinceStart = dayjs(event.timestamp).diff(startDate, "minutes");
    const intervalIndex = Math.min(Math.floor(minutesSinceStart / intervalSize), 9);

    if (intervals[intervalIndex] !== undefined) {
      intervals[intervalIndex] += 1;
    }
  });

  return intervals;
};

const options = {
  layout: {
    padding: {
      bottom: 2,
      top: 2,
    },
  },
  maintainAspectRatio: false,
  plugins: {
    legend: {
      display: false,
    },
    tooltip: {
      enabled: false,
    },
  },
  responsive: true,
  scales: {
    x: {
      display: false,
      grid: {
        display: false,
      },
    },
    y: {
      display: false,
      grid: {
        display: false,
      },
    },
  },
} satisfies ChartOptions;

type Props = {
  readonly endDate: string;
  readonly events: Array<ChartEvent>;
  readonly startDate: string;
};

export const TrendCountChart = ({ endDate, events, startDate }: Props) => {
  const { colorMode } = useColorMode();
  const chartRef = useRef<ChartJS<"line">>();

  // Get raw color values instead of CSS variables
  const [bgLight, bgDark, lineLight, lineDark] = useToken("colors", [
    "red.100",
    "red.800",
    "red.500",
    "red.400",
  ]);

  const backgroundColor = colorMode === "light" ? bgLight : bgDark;
  const lineColor = colorMode === "light" ? lineLight : lineDark;

  const intervalData = useMemo(
    () => aggregateEventsIntoIntervals(events, startDate, endDate),
    [events, startDate, endDate],
  );

  // Cleanup chart instance on unmount
  useEffect(
    () => () => {
      if (chartRef.current) {
        chartRef.current.destroy();
      }
    },
    [],
  );

  const data = {
    datasets: [
      {
        backgroundColor,
        borderColor: lineColor,
        borderWidth: 2,
        data: intervalData,
        fill: true,
        pointRadius: 0,
        tension: 0.4,
      },
    ],
    labels: Array.from({ length: 10 }).fill(""),
  };

  return (
    <Box h="25px" w="200px">
      <Line data={data} options={options} ref={chartRef} />
    </Box>
  );
};
