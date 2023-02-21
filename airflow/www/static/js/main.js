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
/* global $, moment, Airflow, window, localStorage, document, hostName, csrfToken, CustomEvent */

import {
  dateTimeAttrFormat,
  formatTimezone,
  isoDateToTimeEl,
  setDisplayedTimezone,
  TimezoneEvent,
} from './datetime_utils';

window.isoDateToTimeEl = isoDateToTimeEl;

/*
 We pull moment in via a webpack entrypoint rather than import
 so that we don't put it in more than a single .js file.
 This "exports" it to be globally available.
*/
window.moment = Airflow.moment;

function displayTime() {
  const now = moment();
  $('#clock')
    .attr('datetime', now.format(dateTimeAttrFormat))
    .html(`${now.format('HH:mm')} <strong>${formatTimezone(now)}</strong>`);
}

function changeDisplayedTimezone(tz) {
  localStorage.setItem('selected-timezone', tz);

  // dispatch an event that React can listen for
  const event = new CustomEvent(TimezoneEvent, {
    detail: tz,
  });
  document.dispatchEvent(event);

  setDisplayedTimezone(tz);
  displayTime();
  $('body').trigger({
    type: 'airflow.timezone-change',
    timezone: tz,
  });
}

const el = document.createElement('span');

export function escapeHtml(text) {
  el.textContent = text;
  return el.innerHTML;
}

window.escapeHtml = escapeHtml;

export function convertSecsToHumanReadable(seconds) {
  const oriSeconds = seconds;
  const floatingPart = oriSeconds - Math.floor(oriSeconds);

  seconds = Math.floor(seconds);

  const secondsPerHour = 60 * 60;
  const secondsPerMinute = 60;

  const hours = Math.floor(seconds / secondsPerHour);
  seconds -= hours * secondsPerHour;

  const minutes = Math.floor(seconds / secondsPerMinute);
  seconds -= minutes * secondsPerMinute;

  let readableFormat = '';
  if (hours > 0) {
    readableFormat += `${hours}Hours `;
  }
  if (minutes > 0) {
    readableFormat += `${minutes}Min `;
  }
  if (seconds + floatingPart > 0) {
    if (Math.floor(oriSeconds) === oriSeconds) {
      readableFormat += `${seconds}Sec`;
    } else {
      seconds += floatingPart;
      readableFormat += `${seconds.toFixed(3)}Sec`;
    }
  }
  return readableFormat;
}
window.convertSecsToHumanReadable = convertSecsToHumanReadable;

function postAsForm(url, parameters) {
  const form = $('<form></form>');

  form.attr('method', 'POST');
  form.attr('action', url);

  $.each(parameters || {}, (key, value) => {
    const field = $('<input></input>');

    field.attr('type', 'hidden');
    field.attr('name', key);
    field.attr('value', value);

    form.append(field);
  });

  const field = $('<input></input>');

  field.attr('type', 'hidden');
  field.attr('name', 'csrf_token');
  field.attr('value', csrfToken);

  form.append(field);

  // The form needs to be a part of the document in order for us to be able
  // to submit it.
  $(document.body).append(form);
  form.submit();
}

window.postAsForm = postAsForm;

function initializeUITimezone() {
  const local = moment.tz.guess();

  const selectedTz = localStorage.getItem('selected-timezone');
  const manualTz = localStorage.getItem('chosen-timezone');

  function setManualTimezone(tz) {
    localStorage.setItem('chosen-timezone', tz);
    if (tz === local && tz === Airflow.serverTimezone) {
      $('#timezone-manual').hide();
      return;
    }

    $('#timezone-manual a').data('timezone', tz).text(formatTimezone(tz));
    $('#timezone-manual').show();
  }

  if (manualTz) {
    setManualTimezone(manualTz);
  }

  changeDisplayedTimezone(selectedTz || Airflow.defaultUITimezone);

  if (Airflow.serverTimezone !== 'UTC') {
    $('#timezone-server a').html(`${formatTimezone(Airflow.serverTimezone)} <span class="label label-primary">Server</span>`);
    $('#timezone-server').show();
  }

  if (Airflow.serverTimezone !== local) {
    $('#timezone-local a')
      .attr('data-timezone', local)
      .html(`${formatTimezone(local)} <span class="label label-info">Local</span>`);
  } else {
    $('#timezone-local').hide();
  }

  $('a[data-timezone]').click((evt) => {
    changeDisplayedTimezone($(evt.currentTarget).data('timezone'));
  });

  $('#timezone-other').typeahead({
    source: $(moment.tz.names().map((tzName) => {
      const category = tzName.split('/', 1)[0];
      return { category, name: tzName.replace('_', ' '), tzName };
    })),
    showHintOnFocus: 'all',
    showCategoryHeader: true,
    items: 'all',
    afterSelect(data) {
      // Clear it for next time we open the pop-up
      this.$element.val('');

      setManualTimezone(data.tzName);
      changeDisplayedTimezone(data.tzName);

      // We need to delay the close event to not be in the form handler,
      // otherwise bootstrap ignores it, thinking it's caused by interaction on
      // the <form>
      setTimeout(() => {
        document.activeElement.blur();
        // Bug in typeahed, it thinks it's still shown!
        this.shown = false;
        this.focused = false;
      }, 1);
    },
  });
}

function filterOpSelected(ele) {
  const op = $(ele);
  const filterVal = $('.filter_val.form-control', op.parents('tr'));

  if (op.text() === 'Is Null' || op.text() === 'Is not Null') {
    if (filterVal.attr('required') !== undefined) {
      filterVal.removeAttr('required');
      filterVal.attr('airflow-required', true);
    }

    if (filterVal.parent('.datetime').length === 1) {
      filterVal.parent('.datetime').hide();
    } else {
      filterVal.hide();
    }
  } else {
    if (filterVal.attr('airflow-required') === 'true') {
      filterVal.attr('required', true);
      filterVal.removeAttr('airflow-required');
    }

    if (filterVal.parent('.datetime').length === 1) {
      filterVal.parent('.datetime').show();
    } else {
      filterVal.show();
    }
  }
}

$(document).ready(() => {
  initializeUITimezone();

  $('#clock')
    .attr('data-original-title', hostName)
    .attr('data-placement', 'bottom')
    .parent()
    .show();

  displayTime();
  setInterval(displayTime, 1000);

  $.ajaxSetup({
    beforeSend(xhr, settings) {
      if (!/^(GET|HEAD|OPTIONS|TRACE)$/i.test(settings.type) && !this.crossDomain) {
        xhr.setRequestHeader('X-CSRFToken', csrfToken);
      }
    },
  });

  $.fn.datetimepicker.defaults.sideBySide = true;
  $('.datetimepicker').datetimepicker({ format: 'YYYY-MM-DDTHH:mm:ssZ' });
  $('.datepicker').datetimepicker({ format: 'YYYY-MM-DD' });
  $('.timepicker').datetimepicker({ format: 'HH:mm:ss' });

  $('.filters .select2-chosen').each((idx, elem) => { filterOpSelected(elem); });
  $('.filters .select2-chosen').on('DOMNodeInserted', (e) => { filterOpSelected(e.target); });

  // Fix up filter fields from FAB adds to the page. This event is fired after
  // the FAB registered one which adds the new control
  $('#filter_form a.filter').click(() => {
    $('.datetimepicker').datetimepicker();
    $('.filters .select2-chosen').on('DOMNodeInserted', (e) => { filterOpSelected(e.target); });
  });

  // Global Tooltip selector
  $('.js-tooltip').tooltip();
});
