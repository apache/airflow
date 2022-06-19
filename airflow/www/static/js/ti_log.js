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

/* global document, window, $ */
import { escapeHtml } from './main';
import { getMetaValue } from './utils';
import { formatDateTime } from './datetime_utils';

const executionDate = getMetaValue('execution_date');
const dagId = getMetaValue('dag_id');
const taskId = getMetaValue('task_id');
const mapIndex = getMetaValue('map_index');
const logsWithMetadataUrl = getMetaValue('logs_with_metadata_url');
const DELAY = parseInt(getMetaValue('delay'), 10);
const AUTO_TAILING_OFFSET = parseInt(getMetaValue('auto_tailing_offset'), 10);
const ANIMATION_SPEED = parseInt(getMetaValue('animation_speed'), 10);
const TOTAL_ATTEMPTS = parseInt(getMetaValue('total_attempts'), 10);

function recurse(delay = DELAY) {
  return new Promise((resolve) => { setTimeout(resolve, delay); });
}

// Enable auto tailing only when users scroll down to the bottom
// of the page. This prevent auto tailing the page if users want
// to view earlier rendered messages.
function checkAutoTailingCondition() {
  const docHeight = $(document).height();
  console.debug($(window).scrollTop());
  console.debug($(window).height());
  console.debug($(document).height());
  return $(window).scrollTop() !== 0
         && ($(window).scrollTop() + $(window).height() > docHeight - AUTO_TAILING_OFFSET);
}

function toggleWrap() {
  $('pre code').toggleClass('wrap');
}

function scrollBottom() {
  $('html, body').animate({ scrollTop: $(document).height() }, ANIMATION_SPEED);
}

window.toggleWrapLogs = toggleWrap;
window.scrollBottomLogs = scrollBottom;

// Streaming log with auto-tailing.
function autoTailingLog(tryNumber, metadata = null, autoTailing = false) {
  console.debug(`Auto-tailing log for dag_id: ${dagId}, task_id: ${taskId}, `
   + `execution_date: ${executionDate}, map_index: ${mapIndex}, try_number: ${tryNumber}, `
   + `metadata: ${JSON.stringify(metadata)}`);

  return Promise.resolve(
    $.ajax({
      url: logsWithMetadataUrl,
      data: {
        dag_id: dagId,
        task_id: taskId,
        map_index: mapIndex,
        execution_date: executionDate,
        try_number: tryNumber,
        metadata: JSON.stringify(metadata),
      },
    }),
  ).then((res) => {
    // Stop recursive call to backend when error occurs.
    if (!res) {
      document.getElementById(`loading-${tryNumber}`).style.display = 'none';
      return;
    }
    // res.error is a boolean
    // res.message is the log itself or the error message
    if (res.error) {
      if (res.message) {
        console.error(`Error while retrieving log: ${res.message}`);
      }
      document.getElementById(`loading-${tryNumber}`).style.display = 'none';
      return;
    }

    if (res.message) {
      // Auto scroll window to the end if current window location is near the end.
      let shouldScroll = false;
      if (autoTailing && checkAutoTailingCondition()) {
        shouldScroll = true;
      }

      // Detect urls and log timestamps
      const urlRegex = /http(s)?:\/\/[\w.-]+(\.?:[\w.-]+)*([/?#][\w\-._~:/?#[\]@!$&'()*+,;=.%]+)?/g;
      const dateRegex = /(\d{4}[./-]\d{2}[./-]\d{2} \d{2}:\d{2}:\d{2})((,\d{3})|([+-]\d{4} \d{3}ms))/g;
      // above regex is a kind of duplication of 'dateRegex'
      // in airflow/www/static/js/grid/details/content/taskinstance/Logs/utils.js

      res.message.forEach((item) => {
        const logBlockElementId = `try-${tryNumber}-${item[0]}`;
        let logBlock = document.getElementById(logBlockElementId);
        if (!logBlock) {
          const logDivBlock = document.createElement('div');
          const logPreBlock = document.createElement('pre');
          logDivBlock.appendChild(logPreBlock);
          logPreBlock.innerHTML = `<code id="${logBlockElementId}"  ></code>`;
          document.getElementById(`log-group-${tryNumber}`).appendChild(logDivBlock);
          logBlock = document.getElementById(logBlockElementId);
        }

        // The message may contain HTML, so either have to escape it or write it as text.
        const escapedMessage = escapeHtml(item[1]);
        const linkifiedMessage = escapedMessage
          .replace(urlRegex, (url) => `<a href="${url}" target="_blank">${url}</a>`)
          .replaceAll(dateRegex, (dateMatches, date, msecOrUTCOffset) => {
            // e.g) '2022-06-15 10:30:06,020' or '2022-06-15 10:30:06+0900 123ms'
            if (msecOrUTCOffset.startsWith(',')) { // e.g) date='2022-06-15 10:30:06', msecOrUTCOffset=',020'
              // for backward compatibility. (before 2.3.3)
              // keep previous behavior if utcoffset not found.
              //
              return `<time datetime="${dateMatches}+00:00" data-with-tz="true">${formatDateTime(`${dateMatches}+00:00`)}</time>`;
            }
            // e.g) date='2022-06-15 10:30:06', msecOrUTCOffset='+0900 123ms'
            // (formatted by airflow.utils.log.timezone_aware.TimezoneAware) (since 2.3.3)
            const [utcoffset, threeDigitMs] = msecOrUTCOffset.split(' ');
            const msec = threeDigitMs.replace(/\D+/g, ''); // drop 'ms'
            const dateTime = `${date}.${msec}${utcoffset}`; // e.g) datetime='2022-06-15 10:30:06.123+0900'
            //
            return `<time datetime="${dateTime}" data-with-tz="true">${formatDateTime(`${dateTime}`)}</time>`;
          });
        logBlock.innerHTML += `${linkifiedMessage}\n`;
      });

      // Auto scroll window to the end if current window location is near the end.
      if (shouldScroll) {
        scrollBottom();
      }
    }

    if (res.metadata.end_of_log) {
      document.getElementById(`loading-${tryNumber}`).style.display = 'none';
      return;
    }
    recurse().then(() => autoTailingLog(tryNumber, res.metadata, autoTailing));
  });
}

function setDownloadUrl(tryNumber) {
  if (!tryNumber) {
    // default to the currently selected tab
    tryNumber = $('#ti_log_try_number_list .active a').data('try-number');
  }
  const query = new URLSearchParams({
    dag_id: dagId,
    task_id: taskId,
    execution_date: executionDate,
    try_number: tryNumber,
    metadata: 'null',
    format: 'file',
  });
  const url = `${logsWithMetadataUrl}?${query}`;
  $('#ti_log_download_active').attr('href', url);
}

$(document).ready(() => {
  // Lazily load all past task instance logs.
  // TODO: We only need to have recursive queries for
  // latest running task instances. Currently it does not
  // work well with ElasticSearch because ES query only
  // returns at most 10k documents. We want the ability
  // to display all logs in the front-end.
  // An optimization here is to render from latest attempt.
  for (let i = TOTAL_ATTEMPTS; i >= 1; i -= 1) {
    // Only autoTailing the page when streaming the latest attempt.
    const autoTailing = i === TOTAL_ATTEMPTS;
    autoTailingLog(i, null, autoTailing);
  }

  setDownloadUrl();
  $('#ti_log_try_number_list a').click(function () {
    const tryNumber = $(this).data('try-number');
    setDownloadUrl(tryNumber);
  });
});
