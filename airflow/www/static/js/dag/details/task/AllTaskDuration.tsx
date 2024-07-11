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

import { useSearchParams } from "react-router-dom";
import useSelection from "src/dag/useSelection";
import { useGridData } from "src/api";
import { startCase } from "lodash";
import { getDuration, defaultFormat } from "src/datetime_utils";
import ReactECharts, { ReactEChartsProps } from "src/components/ReactECharts";
import URLSearchParamsWrapper from "src/utils/URLSearchParamWrapper";

interface SeriesData {
  name?: string | null;
  type: string;
}

const TAB_PARAM = "tab";

const AllTaskDuration = () => {
  const { onSelect } = useSelection();
  const [searchParams, setSearchParams] = useSearchParams();

  const {
    data: { dagRuns, groups, ordering },
  } = useGridData();
  let maxDuration = 0;
  let unit = "seconds";
  let unitDivisor = 1;
  let map = {
    runId: [],
  };

  dagRuns.forEach((dr) => map["runId"].push(dr.runId));

  const seriesData: Array<SeriesData> = [];
  const legendData: Array<string | null> = [];
  const orderingLabel = ordering[0] || ordering[1] || "startDate";

  groups.children?.forEach((children) => {
    legendData.push(children.id);
    seriesData.push({
      name: children.id,
      type: "line",
    });

    map[children.id] = children.instances.map((instance) => {
      const runDuration = moment
        .duration(
          instance.startDate
            ? getDuration(instance.startDate, instance?.endDate)
            : 0
        )
        .asSeconds();

      if (runDuration > maxDuration) {
        maxDuration = runDuration;
      }

      return runDuration;
    });
  });

  function formatTooltip(args) {
    let { data } = args[0];
    const durations = data.slice(1);
    return durations
      .map(
        (duration, index) =>
          seriesData[index].name +
          " " +
          moment.utc(duration * 1000).format("HH[h]:mm[m]:ss[s]")
      )
      .join("<br>");
  }

  const option: ReactEChartsProps["option"] = {
    legend: {
      orient: "horizontal",
      icon: "circle",
      data: legendData,
    },
    tooltip: {
      trigger: "axis",
      formatter: formatTooltip,
    },
    dataset: {
      dimensions: ["runId"].concat(legendData),
      source: map,
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
        formatter: function (value, index) {
          let duration = moment.utc(value * 1000);
          return duration.format("HH[h]:mm[m]:ss[s]");
        },
      },
    },
  };

  const events = {
    // @ts-ignore
    click(params) {
      const url_params = new URLSearchParamsWrapper(searchParams);
      url_params.set("tab", "details");
      setSearchParams(url_params);

      onSelect({
        taskId: params.seriesName,
        runId: params.name,
      });
    },
  };

  return <ReactECharts option={option} events={events} />;
};

export default AllTaskDuration;
