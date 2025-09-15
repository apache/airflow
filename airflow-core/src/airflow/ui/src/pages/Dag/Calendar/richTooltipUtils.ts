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
import dayjs from "dayjs";

import type { CalendarCellData } from "./types";

export const createRichTooltipContent = (cellData: CalendarCellData) => {
  const { counts, date } = cellData;
  const hasRuns = counts.total > 0;

  if (!hasRuns) {
    return {
      date: dayjs(date).format("MMM DD, YYYY"),
      hasRuns: false,
      states: [],
      total: 0,
    };
  }

  const states = Object.entries(counts)
    .filter(([key, value]) => key !== "total" && value > 0)
    .map(([state, count]) => ({
      color: `var(--chakra-colors-${state}-solid)`,
      count,
      state,
    }));

  return {
    date: dayjs(date).format("MMM DD, YYYY"),
    hasRuns: true,
    states,
    total: counts.total,
  };
};
