/* eslint-disable func-names */
/* eslint-disable no-underscore-dangle */
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

/* global calendarData, document, window, $, d3, moment, call_modal_dag, call_modal, */
import getMetaValue from './meta_value';

const dagId = getMetaValue('dag_id');
const treeUrl = getMetaValue('tree_url');

function getTreeViewURL(d) {
  return treeUrl +
    "?dag_id=" + encodeURIComponent(dagId) +
    "&base_date=" + encodeURIComponent(d.toISOString())
}

// date helpers
function formatDay(d) {
  return ["Su", "Mo", "Tu", "We", "Th", "Fr", "Sa"][d]
}

function formatMonth(m) {
  return ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sept", "Oct", "Nov", "Dec"][m]
}

function toMoment(y, m, d) {
  return moment.utc(`${y}-${m + 1}-${d}`, 'YYYY-MM-DD')
}

function weekOfMonth(y, m, d) {
  const offset = toMoment(y, m, 1).isoWeekday()
  return Math.floor((toMoment(y, m, d).date() - 1 + offset) / 7)
}

function daysInMonth(y, m) {
  const startDay = toMoment(y, m, 1)
  // we substract 1 otherwise moment will return a duration of 1 month and 0 days
  const endDay = toMoment(y, m, 1).add(1, 'month').subtract(1, 'day')
  return moment.duration(endDay.diff(startDay)).asDays() + 1
}

// The state of the days will picked according to the states of the dag runs for that day
// using the following states priority
const priority = ["failed", "upstream_failed", "up_for_retry","up_for_reschedule",
                  "queued", "scheduled", "sensing", "running", "shutdown", "removed",
                  "no_status", "success", "skipped"]

function stateClass(dagStates) {
  for (const state of priority) {
    if (dagStates[state] !== undefined) return state
  }
  return 'no_dag_states'
}

document.addEventListener('DOMContentLoaded', () => {
  $('span.status_square').tooltip({ html: true });

  // JSON.parse is faster for large payloads than an object literal
  const data = JSON.parse(calendarData);

  const dayTip = d3.tip()
    .attr('class', 'tooltip d3-tip')
    .html(function(toolTipHtml) {
      return toolTipHtml;
  });

  // draw the calendar
  function draw() {

    // display constants
    const fullWidth = 1100
    const leftPadding = 40
    const yearPadding = 20
    const yearHeaderHeight = 20
    const monthHeaderHeight = 16
    const cellSize = 16;
    const yearHeight = yearPadding + yearHeaderHeight + monthHeaderHeight + cellSize * 7

    // group dag run stats by year -> month -> day -> state
    let dagStates = d3
      .nest()
      .key(dr => new Date(dr.date).getUTCFullYear())
      .key(dr => new Date(dr.date).getMonth())
      .key(dr => new Date(dr.date).getDate())
      .key(dr => dr.state)
      .map(data.dag_states);

    dagStates = d3
      .entries(dagStates)
      .map(keyVal => ({
        'year': keyVal.key,
        'dagStates': keyVal.value,
      }))
      .sort(data => data.year);

    // root SVG element
    const svg = d3
      .select("#calendar-svg")
      .attr("width", fullWidth)
      .attr("height", yearHeight * dagStates.length)
      .call(dayTip)

    const group = svg.append("g");

    // create the years groups, eahc holding a year of data
    const year = group
      .selectAll("g")
      .data(dagStates)
      .enter()
      .append('g')
      .attr("transform", (d, i) => `translate(0, ${yearHeight * i + yearPadding})`);

    year
      .append("text")
      .attr("x", fullWidth / 2)
      .attr("y", yearHeaderHeight / 2)
      .attr("text-anchor", "middle")
      .attr("font-size", 16)
      .attr("font-weight", 550)
      .text(d => d.year)

    const yearBody = year
      .append('g')
      .attr("transform", `translate(${leftPadding}, ${yearHeaderHeight})`);

    // write day names
    yearBody
      .append("g")
      .attr("text-anchor", "end")
      .attr("transform", `translate(0, ${monthHeaderHeight})`)
      .selectAll("g")
      .data(d3.range(7))
      .enter()
      .append('text')
      .attr("y", i => (i + 0.5) * cellSize)
      .attr("dy", "0.31em")
      .attr("font-size", 12)
      .text(formatDay);

    // create months groups to old the individual day cells
    const months = yearBody
      .append("g")
      .attr("transform", data => `translate(10, 0)`);

    const startWeekOffset = (y, m) => m == 0 ? 0 : toMoment(y, m, 1).isoWeek()
    const endWeekOffset = (y, m) => toMoment(y, m, 1).add(1, 'month').subtract(7, 'day').isoWeek() + 1
    const monthXPos = (y, m) => startWeekOffset(y, m) * cellSize + m * cellSize
    const monthWitdth = (y, m) => (endWeekOffset(y, m) - startWeekOffset(y, m) + 1) * cellSize

    const month = months
      .append("g")
      .selectAll("g")
      .data(data => d3
        .range(12)
        .map(i => {
          const year = data.year
          const month = i
          const dagStatesByDay = data.dagStates[month] || {}
          return {
            'year': year,
            'month': month,
            'dagStates': dagStatesByDay,
          }
        })
      )
      .enter()
      .append('g')
      .attr("transform", data => `translate(${monthXPos(data.year, data.month)}, 0)`);

    // write month names
    month
      .append('text')
      .attr("x", data => monthWitdth(data.year, data.month) / 2)
      .attr("y", monthHeaderHeight / 2)
      .attr("text-anchor", "middle")
      .text(data => formatMonth(data.month))

    const monthBody =  month
      .append("g")
      .attr("transform", data => `translate(0, ${monthHeaderHeight})`)

    // create days inside the month groups
    const tipHtml = data => {
      const stateCounts = d3.entries(data.dagStates).map(kv => `${kv.value[0].count} ${kv.key}`)
      const d = toMoment(data.year, data.month, data.day)
      return `<strong>${d}</strong><br>${stateCounts.join('<br>')}`
    }

    monthBody
      .selectAll("text")
      .data(data => d3
        .range(daysInMonth(data.year, data.month))
        .map(i => {
          const day = i + 1
          const dagStatesByState = data.dagStates[day] || {}
          return {
            'year': data.year,
            'month': data.month,
            'day': day,
            'dagStates': dagStatesByState,
            'weekDay': toMoment(data.year, data.month, day).day(),
            'monthWeek': weekOfMonth(data.year, data.month, day),
          }
        })
      )
      .enter()
      .append('rect')
      .attr("x", data => data.monthWeek  * cellSize)
      .attr("y", data => data.weekDay  * cellSize)
      .attr("width", cellSize)
      .attr("height", cellSize)
      .attr("class", data => stateClass(data.dagStates))
      .style({
        "fill": "#eeeeee",
        "stroke": "#ffffff",
        "stroke-width": 1,
        "cursor": "pointer"
      })
      .on("click", data => {
        window.location.href=getTreeViewURL(
          // add 1 day and substract 1 ms to not show any run from the next day.
          toMoment(data.year, data.month, data.day).add(1, 'day').subtract(1, 'ms')
        )
      })
      .on("mouseover", function(data) {
        const tt = tipHtml(data);
        dayTip.direction('n');
        dayTip.show(tt, this);
      })
      .on('mouseout', function(d,i){dayTip.hide(d, this)});
  }

  function update() {
    $('#loading').remove();
    draw();
  }

  update();
});
