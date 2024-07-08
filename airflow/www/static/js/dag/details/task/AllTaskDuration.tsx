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

import { useGridData } from "src/api";
import { startCase } from "lodash";
import { getDuration, defaultFormat } from "src/datetime_utils";
import ReactECharts, { ReactEChartsProps } from "src/components/ReactECharts";

interface SeriesData {
  name?: string | null;
  type: string;
  data: Array<number>;
}

const AllTaskDuration = () => {
  const {
    data: { dagRuns, groups, ordering },
  } = useGridData();
  let maxDuration = 0;
  let unit = "seconds";
  let unitDivisor = 1;

  const seriesData: Array<SeriesData> = [];
  const legendData: Array<string | null> = [];
  const orderingLabel = ordering[0] || ordering[1] || "startDate";

  groups.children?.forEach((children) => {
    legendData.push(children.id);
    seriesData.push({
      name: children.id,
      type: "line",
      data: children.instances.map((instance) => {
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
      }),
    });
  });

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

  seriesData.forEach((series) => {
    series.data = series.data.map((duration) =>
      (duration / unitDivisor).toFixed(2)
    );
  });

  const option: ReactEChartsProps["option"] = {
    legend: {
      orient: "horizontal",
      icon: "circle",
      data: legendData,
    },
    tooltip: {
      trigger: "axis",
    },
    series: seriesData,
    xAxis: {
      type: "category",
      show: true,
      data: dagRuns.map((dagRun) =>
        // @ts-ignore
        moment(dagRun[orderingLabel]).format(defaultFormat)
      ),
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

  return <ReactECharts option={option} />;
};

export default AllTaskDuration;
