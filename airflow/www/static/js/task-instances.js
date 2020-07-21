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
import { escapeHtml } from './base';
import { defaultFormat, formatDateTime } from './datetime-utils';

function makeDateTimeHTML(start, end) {
  // check task ended or not
  if (end && end instanceof moment) {
    return (
      `Started: ${start.format(defaultFormat)} <br> Ended: ${end.format(defaultFormat)} <br>`
    )
  }
  return (
    `Started: ${start.format(defaultFormat)} <br> Ended: Not ended yet <br>`
  )
}

function generateTooltipDateTimes(startDate, endDate, dagTZ) {
  if (!startDate) {
    return '<br><em>Not yet started</em>';
  }

  const tzFormat = 'z (Z)';
  const localTZ = moment.defaultZone.name;
  startDate = moment.utc(startDate);
  endDate = moment.utc(endDate);
  dagTZ = dagTZ.toUpperCase();

  // Generate UTC Start and End Date
  let tooltipHTML = `<br><strong>UTC:</strong><br>`;
  tooltipHTML += makeDateTimeHTML(startDate, endDate);

  // Generate User's Local Start and End Date
  startDate.tz(localTZ);
  tooltipHTML += `<br><strong>Local: ${startDate.format(tzFormat)}</strong><br>`;
  const localEndDate = endDate && endDate instanceof moment ? endDate.tz(localTZ) : endDate;
  tooltipHTML += makeDateTimeHTML(startDate, localEndDate);

  // Generate DAG's Start and End Date
  if (dagTZ !== 'UTC' && dagTZ !== localTZ) {
    startDate.tz(dagTZ);
    tooltipHTML += `<br><strong>DAG's TZ: ${startDate.format(tzFormat)}</strong><br>`;
    const dagTZEndDate = endDate && endDate instanceof moment ? endDate.tz(dagTZ) : endDate;
    tooltipHTML += makeDateTimeHTML(startDate, dagTZEndDate);
  }

  return tooltipHTML;
}

export default function tiTooltip(ti, {includeTryNumber = false} = {}) {
  let tt = '';
  if (ti.state !== undefined) {
    tt += `<strong>Status:</strong> ${escapeHtml(ti.state)}<br><br>`;
  }
  if (ti.task_id !== undefined) {
    tt += `Task_id: ${escapeHtml(ti.task_id)}<br>`;
  }
  tt += `Run: ${formatDateTime(ti.execution_date)}<br>`;
  if (ti.run_id !== undefined) {
    tt += `Run Id: <nobr>${escapeHtml(ti.run_id)}</nobr><br>`;
  }
  if (ti.operator !== undefined) {
    tt += `Operator: ${escapeHtml(ti.operator)}<br>`;
  }
  // Don't translate/format this, keep it as the full ISO8601 date
  if (ti.start_date instanceof moment) {
    tt += `Started: ${escapeHtml(ti.start_date.toISOString())}<br>`;
  } else {
    tt += `Started: ${escapeHtml(ti.start_date)}<br>`;
  }
  // Calculate duration on the fly if task instance is still running
  if(ti.state === "running") {
    let start_date = ti.start_date instanceof moment ? ti.start_date : moment(ti.start_date);
    ti.duration = moment().diff(start_date, 'second')
  }

  tt += `Duration: ${escapeHtml(convertSecsToHumanReadable(ti.duration))}<br>`;

  if (includeTryNumber) {
    tt += `Try Number: ${escapeHtml(ti.try_number)}<br>`;
  }
  tt += generateTooltipDateTimes(ti.start_date, ti.end_date, dagTZ); // dagTZ has been defined in dag.html
  return tt;
}

window.tiTooltip = tiTooltip
