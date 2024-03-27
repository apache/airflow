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

/* global moment */

import React from "react";
import { startCase } from "lodash";
import type { SeriesOption } from "echarts";

import useSelection from "src/dag/useSelection";
import { useGridData } from "src/api";
import { getDuration, formatDateTime, defaultFormat } from "src/datetime_utils";
import ReactECharts, { ReactEChartsProps } from "src/components/ReactECharts";
import type { DagRun } from "src/types";

interface RunDuration extends DagRun {
  landingDuration: moment.Duration;
  landingDurationUnit: number;
  queuedDuration: moment.Duration;
  queuedDurationUnit: number;
  runDuration: moment.Duration;
  runDurationUnit: number;
}

interface Props {
  showLandingTimes?: boolean;
}

const RunDurationChart = ({ showLandingTimes }: Props) => {
  const { onSelect } = useSelection();

  const {
    data: { dagRuns, ordering },
  } = useGridData();

  let maxDuration = 0;
  let unit = "seconds";
  let unitDivisor = 1;

  const orderingLabel = ordering[0] || ordering[1] || "startDate";

  const durations: (RunDuration | {})[] = dagRuns.map((dagRun) => {
    // @ts-ignore
    const landingDuration = moment.duration(
      getDuration(dagRun.dataIntervalEnd, dagRun.queuedAt || dagRun.startDate)
    );

    // @ts-ignore
    const runDuration = moment.duration(
      dagRun.startDate ? getDuration(dagRun.startDate, dagRun?.endDate) : 0
    );

    // @ts-ignore
    const queuedDuration = moment.duration(
      dagRun.queuedAt && dagRun.startDate && dagRun.startDate > dagRun.queuedAt
        ? getDuration(dagRun.queuedAt, dagRun.startDate)
        : 0
    );

    if (showLandingTimes) {
      if (landingDuration.asSeconds() > maxDuration) {
        maxDuration = landingDuration.asSeconds();
      }
    } else if (runDuration.asSeconds() > maxDuration) {
      maxDuration = runDuration.asSeconds();
    }

    if (maxDuration <= 60 * 2) {
      unit = "seconds";
      unitDivisor = 1;
    } else if (maxDuration <= 60 * 60 * 2) {
      unit = "minutes";
      unitDivisor = 60;
    } else if (maxDuration <= 24 * 60 * 60 * 2) {
      unit = "hours";
      unitDivisor = 60 * 60;
    } else {
      unit = "days";
      unitDivisor = 60 * 60 * 24;
    }

    const landingDurationUnit = landingDuration.asSeconds();
    const queuedDurationUnit = queuedDuration.asSeconds();
    const runDurationUnit = runDuration.asSeconds();

    return {
      ...dagRun,
      landingDuration,
      runDuration,
      queuedDuration,
      landingDurationUnit,
      runDurationUnit,
      queuedDurationUnit,
    };
  });

  // @ts-ignore
  function formatTooltip(args) {
    const { data } = args[0];
    const {
      runId,
      queuedAt,
      startDate,
      logicalDate,
      dataIntervalStart,
      dataIntervalEnd,
      state,
      endDate,
      queuedDurationUnit,
      runDurationUnit,
      landingDurationUnit,
    } = data;

    return `
      Run Id: ${runId} <br>
      State: ${state} <br>
      Logical Date: ${formatDateTime(logicalDate)} <br>
      Data Interval Start: ${formatDateTime(dataIntervalStart)} <br>
      Data Interval End: ${formatDateTime(dataIntervalEnd)} <br>
      ${queuedAt ? `Queued: ${formatDateTime(queuedAt)} <br>` : ""}
      Started: ${startDate && formatDateTime(startDate)} <br>
      Ended: ${endDate && formatDateTime(endDate || undefined)} <br>
      Landing Time: ${landingDurationUnit.toFixed(2)} ${unit}<br>
      ${
        queuedAt
          ? `Queued duration: ${queuedDurationUnit.toFixed(2)} ${unit}<br>`
          : ""
      }
      Run duration: ${runDurationUnit.toFixed(2)} ${unit}<br>
      Total duration: ${(
        landingDurationUnit +
        queuedDurationUnit +
        runDurationUnit
      ).toFixed(2)} ${unit}<br>
    `;
  }

  const option: ReactEChartsProps["option"] = {
    series: [
      ...(showLandingTimes
        ? [
            {
              type: "bar",
              barMinHeight: 0.1,
              itemStyle: {
                color: stateColors.scheduled,
                opacity: 0.6,
              },
              stack: "x",
            } as SeriesOption,
          ]
        : []),
      {
        type: "bar",
        barMinHeight: 0.1,
        itemStyle: {
          color: stateColors.queued,
          opacity: 0.6,
        },
        stack: "x",
      },
      {
        type: "bar",
        barMinHeight: 1,
        itemStyle: {
          opacity: 1,
          // @ts-ignore
          color: (params) => stateColors[params.data.state],
        },
        stack: "x",
      },
    ],
    // @ts-ignore
    dataset: {
      dimensions: [
        "runId",
        ...(showLandingTimes ? ["landingDurationUnit"] : []),
        "queuedDurationUnit",
        "runDurationUnit",
      ],
      source: durations.map((duration) => {
        if (duration) {
          const durationInSeconds = duration as RunDuration;
          return {
            ...durationInSeconds,
            landingDurationUnit:
              durationInSeconds.landingDurationUnit / unitDivisor,
            queuedDurationUnit:
              durationInSeconds.queuedDurationUnit / unitDivisor,
            runDurationUnit: durationInSeconds.runDurationUnit / unitDivisor,
          };
        }
        return duration;
      }),
    },
    tooltip: {
      trigger: "axis",
      formatter: formatTooltip,
      axisPointer: {
        type: "shadow",
      },
    },
    xAxis: {
      type: "category",
      show: true,
      axisLabel: {
        formatter: (runId: string) => {
          const dagRun = dagRuns.find((dr) => dr.runId === runId);
          if (!dagRun || !dagRun[orderingLabel]) return runId;
          // @ts-ignore
          return moment(dagRun[orderingLabel]).format(defaultFormat);
        },
      },
      name: startCase(orderingLabel),
      nameLocation: "end",
      nameGap: 0,
      nameTextStyle: {
        align: "right",
        verticalAlign: "top",
        padding: [30, 0, 0, 0],
      },
    },
    yAxis: {
      type: "value",
      name: `Duration (${unit})`,
    },
  };

  const events = {
    // @ts-ignore
    click(params) {
      onSelect({
        runId: params.data.runId,
      });
    },
  };

  return <ReactECharts option={option} events={events} />;
};

export default RunDurationChart;
