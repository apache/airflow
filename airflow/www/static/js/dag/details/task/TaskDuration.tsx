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

import useSelection from "src/dag/useSelection";
import { useGridData } from "src/api";
import { startCase } from "lodash";
import { getDuration, formatDateTime, defaultFormat } from "src/datetime_utils";
import ReactECharts, { ReactEChartsProps } from "src/components/ReactECharts";
import type { TaskInstance } from "src/types";
import { getTask } from "src/utils";

interface TaskInstanceDuration extends TaskInstance {
  executionDate?: string | null;
  dataIntervalStart?: string | null;
  dataIntervalEnd?: string | null;
  runDuration: moment.Duration;
  runDurationUnit: number;
  queuedDuration: moment.Duration;
  queuedDurationUnit: number;
}

const TaskDuration = () => {
  const {
    selected: { taskId },
    onSelect,
  } = useSelection();

  const {
    data: { dagRuns, groups, ordering },
  } = useGridData();
  let maxDuration = 0;
  let unit = "seconds";
  let unitDivisor = 1;

  const task = getTask({ taskId, task: groups });

  if (!task) return null;
  const orderingLabel = ordering[0] || ordering[1] || "startDate";

  const durations: (TaskInstanceDuration | {})[] = dagRuns.map((dagRun) => {
    const { runId } = dagRun;
    const instance = task.instances.find((ti) => ti && ti.runId === runId);
    if (!instance) return {};
    // @ts-ignore
    const runDuration = moment.duration(
      instance.startDate
        ? getDuration(instance.startDate, instance?.endDate)
        : 0
    );

    // @ts-ignore
    const queuedDuration = moment.duration(
      instance.queuedDttm &&
        instance.startDate &&
        instance.startDate > instance.queuedDttm
        ? getDuration(instance.queuedDttm, instance.startDate)
        : 0
    );

    if (runDuration.asSeconds() > maxDuration) {
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

    const runDurationUnit = runDuration.asSeconds();
    const queuedDurationUnit = queuedDuration.asSeconds();

    return {
      ...instance,
      [orderingLabel]: dagRun ? dagRun[orderingLabel] : instance.startDate,
      runDuration,
      queuedDuration,
      runDurationUnit,
      queuedDurationUnit,
    };
  });

  // @ts-ignore
  function formatTooltip(args) {
    const { data } = args[0];
    const {
      runId,
      queuedDttm,
      startDate,
      state,
      endDate,
      tryNumber,
      queuedDurationUnit,
      runDurationUnit,
    } = data;

    return `
      Run Id: ${runId} <br>
      ${startCase(orderingLabel)}: ${formatDateTime(data[orderingLabel])} <br>
      ${tryNumber && tryNumber > -1 ? `Try Number: ${tryNumber} <br>` : ""}
      State: ${state} <br>
      ${queuedDttm ? `Queued: ${formatDateTime(queuedDttm)} <br>` : ""}
      Started: ${startDate && formatDateTime(startDate)} <br>
      Ended: ${endDate && formatDateTime(endDate || undefined)} <br>
      ${
        queuedDttm
          ? `Queued duration: ${queuedDurationUnit.toFixed(2)} ${unit}<br>`
          : ""
      }
      Run duration: ${runDurationUnit.toFixed(2)} ${unit}<br>
      ${
        queuedDttm
          ? `Total duration: ${(queuedDurationUnit + runDurationUnit).toFixed(
              2
            )} ${unit}<br>`
          : ""
      }
    `;
  }

  function formatMarkLineLegendName(name: string) {
    switch (name) {
      case "runDurationUnit":
        return "Median total duration";
      case "queuedDurationUnit":
        return "Median queued duration";
      default:
        return name;
    }
  }

  const option: ReactEChartsProps["option"] = {
    legend: {
      orient: "horizontal",
      icon: "circle",
      formatter: formatMarkLineLegendName,
      data: [
        {
          name: "runDurationUnit",
          itemStyle: { color: "blue" },
        },
        {
          name: "queuedDurationUnit",
          itemStyle: { color: stateColors.queued },
        },
      ],
    },
    series: [
      {
        type: "bar",
        barMinHeight: 0.1,
        itemStyle: {
          color: stateColors.queued,
          opacity: 0.6,
        },
        stack: "x",
        markLine: {
          silent: true,
          data: [{ type: "median", lineStyle: { color: stateColors.queued } }],
        },
      },
      {
        type: "bar",
        barMinHeight: 1,
        itemStyle: {
          // @ts-ignore
          color: (params) => stateColors[params.data.state],
        },
        stack: "x",
        markLine: {
          silent: true,
          data: [{ type: "median", lineStyle: { color: "blue" } }],
        },
      },
    ],
    // @ts-ignore
    dataset: {
      dimensions: ["runId", "queuedDurationUnit", "runDurationUnit"],
      source: durations.map((duration) => {
        if (duration) {
          const durationInSeconds = duration as TaskInstanceDuration;
          return {
            ...durationInSeconds,
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
        taskId: params.data.taskId,
        runId: params.data.runId,
      });
    },
  };

  return <ReactECharts option={option} events={events} />;
};

export default TaskDuration;
