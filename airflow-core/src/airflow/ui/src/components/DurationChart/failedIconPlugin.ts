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
import type { Chart } from "chart.js";

const FAILED_ICON_PLUGIN_ID = "durationChartFailedIcon";
const ICON_SIZE = 14;
const ICON_OFFSET = 4;

export const createFailedIconPlugin = (failedIndices: Array<number>, failedIconColor: string) => ({
  afterDatasetsDraw(chart: Chart) {
    if (failedIndices.length === 0) {
      return;
    }

    const { ctx } = chart;
    const meta = chart.getDatasetMeta(1);

    if (meta.data.length === 0) {
      return;
    }

    failedIndices.forEach((index) => {
      const element = meta.data[index];

      if (!element) {
        return;
      }

      const { x, y } = element.getProps(["x", "y"], true) as { x: number; y: number };
      const iconX = x;
      const iconY = y - ICON_OFFSET - ICON_SIZE;

      ctx.save();

      const half = ICON_SIZE / 2;

      ctx.beginPath();
      ctx.moveTo(iconX, iconY + ICON_SIZE);
      ctx.lineTo(iconX - half, iconY);
      ctx.lineTo(iconX + half, iconY);
      ctx.closePath();
      ctx.fillStyle = failedIconColor;
      ctx.fill();
      ctx.strokeStyle = failedIconColor;
      ctx.lineWidth = 1;
      ctx.stroke();

      ctx.fillStyle = "white";
      ctx.font = `bold ${ICON_SIZE * 0.6}px sans-serif`;
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillText("!", iconX, iconY + ICON_SIZE * 0.55);

      ctx.restore();
    });
  },
  id: FAILED_ICON_PLUGIN_ID,
});
