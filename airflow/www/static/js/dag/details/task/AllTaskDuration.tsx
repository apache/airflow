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
import type { SeriesOption } from "echarts";

import { useSearchParams } from "react-router-dom";
import useSelection from "src/dag/useSelection";
import { useGridData } from "src/api";
import { startCase } from "lodash";
import { getDuration, defaultFormat } from "src/datetime_utils";
import ReactECharts, { ReactEChartsProps } from "src/components/ReactECharts";
import URLSearchParamsWrapper from "src/utils/URLSearchParamWrapper";

const TAB_PARAM = "tab";

interface Props {
  showBar: boolean;
}

const AllTaskDuration = ({ showBar }: Props) => {
  const { onSelect } = useSelection();
  const [searchParams, setSearchParams] = useSearchParams();

  const {
    data: { dagRuns, groups, ordering },
  } = useGridData();
  const runIds: Array<string> = [];
  const source: Record<string, Array<string | number>> = {};

  dagRuns.forEach((dr) => {
    runIds.push(dr.runId!!);
  });

  source.runId = runIds;
  const seriesData: Array<SeriesOption> = [];
  const legendData: Array<string> = [];
  const orderingLabel = ordering[0] || ordering[1] || "startDate";

  groups.children?.forEach((children) => {
    if (children.id === null) {
      return;
    }

    const taskId = children.id;
    legendData.push(taskId);

    if (showBar) {
      seriesData.push({
        name: taskId,
        type: "bar",
        stack: "x",
      } as SeriesOption);
    } else {
      seriesData.push({
        name: taskId,
        type: "line",
      } as SeriesOption);
    }

    source[taskId] = children.instances.map((instance) => {
      // @ts-ignore
      const runDuration = moment
        .duration(
          instance.startDate
            ? getDuration(instance.startDate, instance?.endDate)
            : 0
        )
        .asSeconds();

      return runDuration;
    });
  });

  const dimensions = ["runId"].concat(legendData);

  // @ts-ignore
  function formatTooltip(value) {
    // @ts-ignore
    return moment.utc(value * 1000).format("HH[h]:mm[m]:ss[s]");
  }

  const option: ReactEChartsProps["option"] = {
    legend: {
      orient: "horizontal",
      type: "scroll",
      icon: "circle",
      data: legendData,
    },
    tooltip: {
      trigger: "axis",
      valueFormatter: formatTooltip,
    },
    // @ts-ignore
    asset: {
      dimensions,
      source,
    },
    series: seriesData,
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
      name: `Duration`,
      axisLabel: {
        formatter(value: number) {
          // @ts-ignore
          const duration = moment.utc(value * 1000);
          return duration.format("HH[h]:mm[m]:ss[s]");
        },
      },
    },
  };

  const events = {
    // @ts-ignore
    click(params) {
      const URL_PARAMS = new URLSearchParamsWrapper(searchParams);
      URL_PARAMS.set(TAB_PARAM, "details");
      setSearchParams(URL_PARAMS);

      onSelect({
        taskId: params.seriesName,
        runId: params.name,
      });
    },
  };

  return <ReactECharts option={option} events={events} />;
};

export default AllTaskDuration;
