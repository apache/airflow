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
/* global $, tiTooltip, taskTip, d3, getTaskInstanceURL, taskInstances */

// Assigning css classes based on state to nodes
// Initiating the tooltips
function updateNodesStates(taskInstances) {
  $.each(taskInstances, (taskId, ti) => {
    $('tspan').filter(() => $(this).text() === taskId)
      .parent()
      .parent()
      .parent()
      .parent()
      .attr('class', `node enter ${ti.state ? ti.state : 'no_status'}`)
      .attr('data-toggle', 'tooltip')
      .on('mouseover', () => {
        const tt = tiTooltip(taskInstances[taskId]);
        taskTip.show(tt, this);
      })
      .on('mouseout', taskTip.hide);
  });
}

function initRefreshButton() {
  d3.select('#refresh_button').on('click', () => {
    $('#loading').css('display', 'block');
    $('div#svg_container').css('opacity', '0.2');
    $.get(getTaskInstanceURL)
      .done((taskInstances) => {
        updateNodesStates(JSON.parse(taskInstances));
        $('#loading').hide();
        $('div#svg_container').css('opacity', '1');
        $('#error').hide();
      }).fail((jqxhr, textStatus, err) => {
        $('#error_msg').html(`${textStatus}: ${err}`);
        $('#error').show();
        $('#loading').hide();
        $('#chart_section').hide(1000);
        $('#datatable_section').hide(1000);
      });
  });
}

initRefreshButton();
updateNodesStates(taskInstances);
