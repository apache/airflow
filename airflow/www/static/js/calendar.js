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

/* global calendarData, document, window, $, d3, moment */
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
  return ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"][d]
}

function toMoment(y, m, d) {
  return moment.utc([y, m, d])
}

function weekOfMonth(y, m, d) {
  const monthOffset = toMoment(y, m, 1).day()
  const dayOfMonth = toMoment(y, m, d).date()
  return Math.floor((dayOfMonth + monthOffset - 1) / 7)
}

function weekOfYear(y, m) {
  const yearOffset = toMoment(y, 0, 1).day()
  const dayOfYear = toMoment(y, m, 1).dayOfYear()
  return Math.floor((dayOfYear + yearOffset - 1) / 7)
}

function daysInMonth(y, m) {
  const lastDay = toMoment(y, m, 1).add(1, 'month').subtract(1, 'day')
  return lastDay.date()
}

function weeksInMonth(y, m) {
  const firstDay = toMoment(y, m, 1)
  const monthOffset = firstDay.day()
  return Math.floor((daysInMonth(y, m) + monthOffset) / 7) + 1
}

const dateFormat = 'YYYY-MM-DD'

// The state of the days will picked according to the states of the dag runs for that day
// using the following states priority:
const priority = ["failed", "running", "success"]

function stateClass(dagStates) {
  for (const state of priority) {
    if (dagStates[state] !== undefined) return state
  }
  return 'no_status'
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
    const leftPadding = 32
    const yearLabelWidth = 34
    const dayLabelWidth = 14
    const dayLabelPadding = 4
    const yearPadding = 20
    const cellSize = 16
    const yearHeight = cellSize * 7 + 2
    const maxWeeksInYear = 53
    const fullWidth = leftPadding * 2 + yearLabelWidth + dayLabelWidth + maxWeeksInYear * cellSize

    // group dag run stats by year -> month -> day -> state
    let dagStates = d3
      .nest()
      .key(dr => moment.utc(dr.date, dateFormat).year())
      .key(dr => moment.utc(dr.date, dateFormat).month())
      .key(dr => moment.utc(dr.date, dateFormat).date())
      .key(dr => dr.state)
      .map(data.dag_states);

    // Make sure we have one year displayed for each year between the start and end dates.
    // This also ensures we do not have show an empty calendar view when no dag runs exist.
    const startYear = moment.utc(data.start_date, dateFormat).year()
    const endYear = moment.utc(data.end_date, dateFormat).year()
    for (let y = startYear; y <= endYear; y++) {
      dagStates[y] = dagStates[y] || {}
    }

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
      .attr("height", (yearHeight + yearPadding) * dagStates.length + yearPadding)
      .call(dayTip)

    const group = svg.append("g");

    // create the years groups, each holding a year of data
    const year = group
      .selectAll("g")
      .data(dagStates)
      .enter()
      .append('g')
      .attr("transform", (d, i) => `translate(${leftPadding}, ${yearPadding + (yearHeight + yearPadding) * i})`);

    year
      .append("text")
      .attr("x", - yearHeight * 0.5)
      .attr("transform", "rotate(270)")
      .attr("text-anchor", "middle")
      .attr("class", "year-label")
      .text(d => d.year);

    // write day names
    year
      .append("g")
      .attr("transform", `translate(${yearLabelWidth}, ${dayLabelPadding})`)
      .attr("text-anchor", "end")
      .selectAll("g")
      .data(d3.range(7))
      .enter()
      .append('text')
      .attr("y", i => (i + 0.5) * cellSize)
      .attr("class", "day-label")
      .text(formatDay);

    // create months groups to old the individual day cells
    const months = year
      .append("g")
      .attr("transform", data => `translate(${yearLabelWidth + dayLabelWidth}, 0)`);

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
      .attr("transform", data => `translate(${weekOfYear(data.year, data.month) * cellSize}, 0)`);

    // create days inside the month groups
    const tipHtml = data => {
      const stateCounts = d3.entries(data.dagStates).map(kv => `${kv.value[0].count} ${kv.key}`)
      const date = toMoment(data.year, data.month, data.day)
      const daySr = formatDay(date.day())
      const dateStr = date.format(dateFormat)
      return `<strong>${daySr} ${dateStr}</strong><br>${stateCounts.join('<br>')}`
    }

    month
      .selectAll("g")
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
          }
        })
      )
      .enter()
      .append('rect')
      .attr("x", data => weekOfMonth(data.year, data.month, data.day)  * cellSize)
      .attr("y", data => toMoment(data.year, data.month, data.day).day()  * cellSize)
      .attr("width", cellSize)
      .attr("height", cellSize)
      .attr("class", data => 'day ' + stateClass(data.dagStates))
      .on("click", data => {
        window.location.href = getTreeViewURL(
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

    // add paths around months
    month
      .selectAll("g")
      .data(data => [data])
      .enter()
      .append('path')
      .attr('class', 'month')
      .style("fill", "none")
      .attr("d", data => {
        const firstDayOffset = toMoment(data.year, data.month, 1).day()
        const lastDayOffset = toMoment(data.year, data.month, 1).add(1, 'month').day()
        const weeks = weeksInMonth(data.year, data.month)
        return d3.svg.line()([
          [0, firstDayOffset * cellSize],
          [cellSize, firstDayOffset * cellSize],
          [cellSize, 0],
          [weeks * cellSize, 0],
          [weeks * cellSize, lastDayOffset * cellSize],
          [(weeks - 1) * cellSize, lastDayOffset * cellSize],
          [(weeks - 1) * cellSize, 7 * cellSize],
          [0, 7 * cellSize],
          [0, firstDayOffset * cellSize]
        ])
      })
  }

  function update() {
    $('#loading').remove();
    draw();
  }

  update();
});
