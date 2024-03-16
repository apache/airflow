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

import React, { useRef, useEffect } from "react";
import { init, getInstanceByDom } from "echarts";
import type { CSSProperties } from "react";
import type { EChartsOption, ECharts, SetOptionOpts } from "echarts";

export interface ReactEChartsProps {
  loading?: boolean;
  option: EChartsOption;
  settings?: SetOptionOpts;
  style?: CSSProperties;
  theme?: "light" | "dark";
  events?: { [key: string]: (params: any) => void };
}

const ReactECharts = ({
  loading,
  option,
  settings,
  style,
  theme,
  events,
}: ReactEChartsProps) => {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    // Init Chart
    let chart: ECharts | undefined;
    if (ref.current !== null) {
      chart = init(ref.current, theme, {
        renderer: "svg",
      });
    }

    const resizeChart = () => {
      chart?.resize();
    };

    window.addEventListener("resize", resizeChart);

    return () => {
      chart?.dispose();
      window.removeEventListener("resize", resizeChart);
    };
  }, [theme]);

  useEffect(() => {
    // Handle chart loading
    if (ref.current !== null) {
      const chart = getInstanceByDom(ref.current);
      if (loading === true) {
        chart?.showLoading();
      } else {
        chart?.hideLoading();
      }
    }
  }, [loading]);

  useEffect(() => {
    // Handle option and theme updates
    if (ref.current !== null) {
      const chartInstance = getInstanceByDom(ref.current);
      if (chartInstance) {
        chartInstance.setOption(option, settings);

        if (events) {
          Object.keys(events).forEach((key) => {
            chartInstance.on(key, events[key]);
          });
        }
      }
    }
  }, [option, settings, theme, events]);

  return (
    <div
      data-testid="echart-container"
      ref={ref}
      style={{ width: "100%", height: "100%", ...style }}
    />
  );
};
export default ReactECharts;
