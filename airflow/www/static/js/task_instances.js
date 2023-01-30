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

/* global window, moment, convertSecsToHumanReadable */

// We don't re-import moment again, otherwise webpack will include it twice in the bundle!
import { escapeHtml } from './main';
import { defaultFormat, formatDateTime } from './datetime_utils';
import { dagTZ } from './dag';
import { finalStatesMap } from './utils';

function makeDateTimeHTML(start, end) {
  // check task ended or not
  const isEnded = end && end instanceof moment && end.isValid();
  return `Started: ${start.format(defaultFormat)}<br>Ended: ${isEnded ? end.format(defaultFormat) : 'Not ended yet'}<br>`;
}

function generateTooltipDateTimes(startTime, endTime, dagTimezone) {
  if (!startTime) {
    return '<br><em>Not yet started</em>';
  }

  const tzFormat = 'z (Z)';
  const localTZ = moment.defaultZone.name.toUpperCase();
  const startDate = moment.utc(startTime);
  const endDate = moment.utc(endTime);
  const dagTz = dagTimezone.toUpperCase();

  // Generate UTC Start and End Date
  let tooltipHTML = '<br><strong>UTC:</strong><br>';
  tooltipHTML += makeDateTimeHTML(startDate, endDate);

  // Generate User's Local Start and End Date, unless it's UTC
  if (localTZ !== 'UTC') {
    startDate.tz(localTZ);
    tooltipHTML += `<br><strong>Local: ${startDate.format(tzFormat)}</strong><br>`;
    const localEndDate = endDate && endDate instanceof moment ? endDate.tz(localTZ) : endDate;
    tooltipHTML += makeDateTimeHTML(startDate, localEndDate);
  }

  // Generate DAG's Start and End Date
  if (dagTz !== 'UTC' && dagTz !== localTZ) {
    startDate.tz(dagTz);
    tooltipHTML += `<br><strong>DAG's TZ: ${startDate.format(tzFormat)}</strong><br>`;
    const dagTZEndDate = endDate && endDate instanceof moment ? endDate.tz(dagTz) : endDate;
    tooltipHTML += makeDateTimeHTML(startDate, dagTZEndDate);
  }

  return tooltipHTML;
}

export default function tiTooltip(ti, task, { includeTryNumber = false } = {}) {
  let tt = '';
  if (ti.state !== undefined) {
    tt += `<strong>Status:</strong> ${escapeHtml(ti.state)}<br><br>`;
  }
  if (ti.mapped_states) {
    const numMap = finalStatesMap();
    ti.mapped_states.forEach((s) => {
      const stateKey = s || 'no_status';
      if (numMap.has(stateKey)) numMap.set(stateKey, numMap.get(stateKey) + 1);
    });
    tt += `<strong>${escapeHtml(ti.mapped_states.length)} ${ti.mapped_states.length === 1 ? 'Task' : 'Tasks'} Mapped</strong><br />`;
    numMap.forEach((key, val) => {
      if (key > 0) {
        tt += `<span style="margin-left: 15px">${escapeHtml(val)}: ${escapeHtml(key)}</span><br />`;
      }
    });
    tt += '<br />';
  }
  if (ti.task_id !== undefined) {
    tt += `Task_id: ${escapeHtml(ti.task_id)}<br>`;
  }
  tt += `Run: ${formatDateTime(ti.execution_date)}<br>`;
  if (ti.run_id !== undefined) {
    tt += `Run Id: <nobr>${escapeHtml(ti.run_id)}</nobr><br>`;
  }
  // Show mapped index for specific child instance, but not for a summary instance
  if (ti.map_index >= 0 && !ti.mapped_states) {
    tt += `Map Index: ${escapeHtml(ti.map_index)}<br>`;
  }
  if (ti.operator !== undefined) {
    tt += `Operator: ${escapeHtml(ti.operator)}<br>`;
  }
  if (task && task.trigger_rule) {
    tt += `Trigger Rule: ${task.trigger_rule}<br>`;
  }
  // Calculate duration on the fly if task instance is still running
  if (ti.state === 'running') {
    const startDate = ti.start_date instanceof moment ? ti.start_date : moment(ti.start_date);
    ti.duration = moment().diff(startDate, 'second');
  } else if (!ti.duration && ti.end_date) {
    const startDate = ti.start_date instanceof moment ? ti.start_date : moment(ti.start_date);
    const endDate = ti.end_date instanceof moment ? ti.end_date : moment(ti.end_date);
    ti.duration = moment(endDate).diff(startDate, 'second');
  }

  tt += `Duration: ${escapeHtml(convertSecsToHumanReadable(ti.duration))}<br>`;

  const intervalStart = ti.data_interval_start;
  const intervalEnd = ti.data_interval_end;
  if (intervalStart && intervalEnd) {
    tt += '<br><strong>Data Interval:</strong><br>';
    tt += `Start: ${formatDateTime(intervalStart)}<br>`;
    tt += `End: ${formatDateTime(intervalEnd)}<br>`;
  }

  if (includeTryNumber) {
    tt += `Try Number: ${escapeHtml(ti.try_number)}<br>`;
  }
  // dagTZ has been defined in dag.html
  tt += generateTooltipDateTimes(ti.start_date, ti.end_date, dagTZ || 'UTC');
  return tt;
}

export function taskNoInstanceTooltip(taskId, task) {
  let tt = '';
  if (taskId) {
    tt += `Task_id: ${escapeHtml(taskId)}<br>`;
  }
  if (task.task_type !== undefined) {
    tt += `Operator: ${escapeHtml(task.task_type)}<br>`;
  }
  tt += '<br><em>DAG has yet to run.</em>';
  return tt;
}

window.tiTooltip = tiTooltip;
window.taskNoInstanceTooltip = taskNoInstanceTooltip;
