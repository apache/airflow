/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { generateTooltipDateTime, moment, formatDateStr, secondsToString } from './datetime-utils';

// Assigning css classes based on state to nodes
// Initiating the tooltips
export const update_nodes_states = (task_instances, dag_tz) => {
  $.each(task_instances, function (task_id, ti) {
    $('tspan').filter(function (index) {
      return $(this).text() === task_id;
    })
      .parent().parent().parent().parent()
      .attr("class", "node enter " + (ti.state ? ti.state : "no_status"))
      .attr("data-toggle", "tooltip")
      .attr("data-original-title", function (d) {
        // Tooltip
        const task = tasks[task_id];
        let tt = "Task_id: " + ti.task_id + "<br>";
        tt += "Run: " + formatDateStr(ti.execution_date, 'UTC') + "<br>";
        if (ti.run_id != undefined) {
          tt += "run_id: <nobr>" + ti.run_id + "</nobr><br>";
        }
        tt += "Operator: " + task.task_type + "<br>";
        tt += "Duration: " + secondsToString(ti.duration) + "<br>";
        tt += "State: " + ti.state + "<br>";
        tt += generateTooltipDateTime(ti.start_date, ti.end_date, dag_tz);
        return tt;
      });
  });
}

export const initRefreshButton = (dagTz) => {
  d3.select("#refresh_button").on("click",
    function () {
      $("#loading").css("display", "block");
      $("div#svg_container").css("opacity", "0.2");
      $.get(getTaskInstanceURL)
        .done(
          function (task_instances) {
            update_nodes_states(JSON.parse(task_instances), dagTz);
            $("#loading").hide();
            $("div#svg_container").css("opacity", "1");
            $('#error').hide();
          }
        ).fail(function (jqxhr, textStatus, err) {
          $('#error_msg').html(textStatus + ': ' + err);
          $('#error').show();
          $('#loading').hide();
          $('#chart_section').hide(1000);
          $('#datatable_section').hide(1000);
        });
    }
  );
}

const localizeExecutionDate = (option, tz) => {
  let ts = moment(option.val()).tz(tz);
  const prefixes = ["scheduled__", "backfill_", "manual__"];
  prefixes.forEach(function(prefix) {
    if (option.text().startsWith(prefix)) {
      option.text(prefix + ts.format());
    }
  });
}

const getNaiveDateString = (picker) => {
  return moment(picker.getDate()).utc().format('YYYY-MM-DD HH:mm');
}

const getSelectedTz = () => {
  return $('#datetimepicker').data('current-tz');
}

const getBaseDateInUtc = () => {
  let picker = $('#datetimepicker').data('datetimepicker');
  let dateString = getNaiveDateString(picker);
  let utcTime = moment.tz(dateString, getSelectedTz()).utc();
  return utcTime;
}

$(document).ready(function () {
  let queryParams = new URLSearchParams(window.location.search)
  let baseDateString = queryParams.get('base_date');
  let baseDate = baseDateString ? moment.tz(baseDateString, 'UTC') : moment().utc();
  $('#datetimepicker')
    .data('current-tz', 'UTC')
    .datetimepicker('setDate', baseDate.toDate());

  $('select#execution_date').on('tz-changed', function(event, tz) {
    $(this).children('option').each(function (i) {
      localizeExecutionDate($(this), tz);
    });
  });

  $('#datetimepicker')
    .addClass('tz-aware')
    .on('tz-changed', function(event, tz) {
      let oldTz = $(this).data('current-tz') || 'UTC';
      $(this).data('current-tz', tz);

      let picker = $(this).data('datetimepicker');
      let inTargetTz = moment.tz(getNaiveDateString(picker), oldTz).tz(tz);
      let naiveDateStrInTargetTz = inTargetTz.format('YYYY-MM-DD HH:mm');

      let actualDate = moment.tz(naiveDateStrInTargetTz, tz);
      let adjustedDisplayDate = moment.tz(naiveDateStrInTargetTz, 'UTC');
      $(this).data('utc-time', actualDate.utc());

      picker.setDate(adjustedDisplayDate);
    });

  $("form").on('submit', function(event) {
    $('#base_date').val(getBaseDateInUtc().format());
  });
});
