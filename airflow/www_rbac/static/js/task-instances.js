/**
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

/* global window, dagTZ, moment, convertSecsToHumanReadable */

// We don't re-import moment again, otherwise webpack will include it twice in the bundle!
import { defaultFormat, formatDateTime, getCurrentTimezone } from './datetime-utils';
import { escapeHtml } from './base';

function makeDateTimeHTML(start, end) {
  return (
    `Started: ${start.format(defaultFormat)} <br> Ended: ${end.format(defaultFormat)} <br>`
  )
}

function generateTooltipDateTimes(startDate, endDate, dagTZ) {
  const tzFormat = 'z (Z)';
  const localTZ = getCurrentTimezone();
  startDate = moment.utc(startDate);
  endDate = moment.utc(endDate);
  dagTZ = dagTZ.toUpperCase();

  // Generate UTC Start and End Date
  if (!startDate) {
    return '<br><em>Not yet started</em>';
  }

  // Generate UTC Start and End Date
  let tooltipHTML = `<br><strong>UTC:</strong><br>`;
  tooltipHTML += makeDateTimeHTML(startDate.local(), endDate.local());

  // Generate User's Local Start and End Date
  tooltipHTML += `<br><strong>Local: ${moment.tz(localTZ).format(tzFormat)}</strong><br>`;
  tooltipHTML += makeDateTimeHTML(startDate.local(), endDate.local());

  // Generate DAG's Start and End Date
  if (dagTZ !== 'UTC' && dagTZ !== localTZ) {
    tooltipHTML += `<br><strong>DAG's TZ: ${moment.tz(dagTZ).format(tzFormat)}</strong><br>`;
    tooltipHTML += makeDateTimeHTML(startDate.tz(dagTZ), endDate.tz(dagTZ));
  }

  return tooltipHTML;
}

export default function tiTooltip(ti) {
  let tt = '';
  if(ti.task_id !== undefined) {
    tt += `Task_id: ${escapeHtml(ti.task_id)}<br>`;
  }
  tt += `Run: ${formatDateTime(ti.execution_date)}<br>`;
  if(ti.run_id !== undefined) {
    tt += `Run Id: <nobr>${escapeHtml(ti.run_id)}</nobr><br>`;
  }
  if(ti.operator !== undefined) {
    tt += `Operator: ${escapeHtml(ti.operator)}<br>`;
  }
  // Don't translate/format this, keep it as the full ISO8601 date
  tt += `Started: ${escapeHtml(ti.start_date)}<br>`;
  tt += `Duration: ${escapeHtml(convertSecsToHumanReadable(ti.duration))}<br>`;
  tt += generateTooltipDateTimes(ti.start_date, ti.end_date, dagTZ); // dagTZ has been defined in dag.html
  return tt;
}

window.tiTooltip = tiTooltip
