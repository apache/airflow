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

/* global document, window, CustomEvent, $ */

import { getMetaValue } from './utils';
import { approxTimeFromNow, formatDateTime } from './datetime_utils';
import { openDatasetModal, getDatasetTooltipInfo } from './datasetUtils';

function updateQueryStringParameter(uri, key, value) {
  const re = new RegExp(`([?&])${key}=.*?(&|$)`, 'i');
  const separator = uri.indexOf('?') !== -1 ? '&' : '?';
  if (uri.match(re)) {
    return uri.replace(re, `$1${key}=${value}$2`);
  }

  return `${uri}${separator}${key}=${value}`;
}

const dagId = getMetaValue('dag_id');
export const dagTZ = getMetaValue('dag_timezone');
const logsWithMetadataUrl = getMetaValue('logs_with_metadata_url');
const externalLogUrl = getMetaValue('external_log_url');
const extraLinksUrl = getMetaValue('extra_links_url');
const pausedUrl = getMetaValue('paused_url');
const nextRun = {
  createAfter: getMetaValue('next_dagrun_create_after'),
  intervalStart: getMetaValue('next_dagrun_data_interval_start'),
  intervalEnd: getMetaValue('next_dagrun_data_interval_end'),
};
let taskId = '';
let executionDate = '';
let subdagId = '';
let dagRunId = '';
let mapIndex;
let mapStates = [];
let extraLinks;
const showExternalLogRedirect = getMetaValue('show_external_log_redirect') === 'True';
let nextDatasets = [];
let nextDatasetsError;

const setNextDatasets = (datasets, error) => {
  nextDatasets = datasets;
  nextDatasetsError = error;
};

// Pills highlighting
$(window).on('load', function onLoad() {
  $(`a[href*="${this.location.pathname}"]`).parent().addClass('active');
  $('.never_active').removeClass('active');
  const run = $('#next-dataset-tooltip');
  getDatasetTooltipInfo(dagId, run, setNextDatasets);
});

const buttons = Array.from(document.querySelectorAll('a[id^="btn_"][data-base-url]')).reduce((obj, elm) => {
  obj[elm.id.replace('btn_', '')] = elm;
  return obj;
}, {});

function updateButtonUrl(elm, params) {
  let url = elm.dataset.baseUrl;
  if (params.dag_id && elm.dataset.baseUrl.indexOf(dagId) !== -1) {
    url = url.replace(dagId, params.dag_id);
    delete params.dag_id;
  }
  if (Object.prototype.hasOwnProperty.call(params, 'map_index') && params.map_index === undefined) {
    delete params.map_index;
  }
  elm.setAttribute('href', `${url}?${$.param(params)}`);
}

function updateModalUrls() {
  updateButtonUrl(buttons.subdag, {
    dag_id: subdagId,
    execution_date: executionDate,
  });

  updateButtonUrl(buttons.task, {
    dag_id: dagId,
    task_id: taskId,
    execution_date: executionDate,
    map_index: mapIndex,
  });

  updateButtonUrl(buttons.rendered, {
    dag_id: dagId,
    task_id: taskId,
    execution_date: executionDate,
    map_index: mapIndex,
  });

  updateButtonUrl(buttons.mapped, {
    _flt_3_dag_id: dagId,
    _flt_3_task_id: taskId,
    _flt_3_run_id: dagRunId,
    _oc_TaskInstanceModelView: 'map_index',
  });

  if (buttons.rendered_k8s) {
    updateButtonUrl(buttons.rendered_k8s, {
      dag_id: dagId,
      task_id: taskId,
      execution_date: executionDate,
      map_index: mapIndex,
    });
  }

  const tiButtonParams = {
    _flt_3_dag_id: dagId,
    _flt_3_task_id: taskId,
    _oc_TaskInstanceModelView: 'dag_run.execution_date',
  };
  // eslint-disable-next-line no-underscore-dangle
  if (mapIndex >= 0) tiButtonParams._flt_0_map_index = mapIndex;
  updateButtonUrl(buttons.ti, tiButtonParams);

  updateButtonUrl(buttons.log, {
    dag_id: dagId,
    task_id: taskId,
    execution_date: executionDate,
    map_index: mapIndex,
  });

  updateButtonUrl(buttons.xcom, {
    dag_id: dagId,
    task_id: taskId,
    execution_date: executionDate,
    map_index: mapIndex,
  });
}

// Update modal urls on toggle
document.addEventListener('click', (event) => {
  if (event.target.matches('button[data-toggle="button"]')) {
    updateModalUrls();
  }
});

export function callModal({
  taskId: t,
  executionDate: d,
  extraLinks: e,
  tryNumber,
  isSubDag,
  dagRunId: drID,
  mapIndex: mi,
  isMapped = false,
  mappedStates = [],
}) {
  taskId = t;
  const location = String(window.location);
  $('#btn_filter').on('click', () => {
    window.location = updateQueryStringParameter(location, 'root', taskId);
  });
  executionDate = d;
  dagRunId = drID;
  mapIndex = mi;
  extraLinks = e;
  if (isMapped) {
    mapStates = mappedStates;
  }
  $('#dag_run_id').text(drID);
  $('#task_id').text(t);
  $('#execution_date').text(formatDateTime(d));
  $('#taskInstanceModal').modal({});
  $('#taskInstanceModal').css('margin-top', '0');
  $('#extra_links').prev('hr').hide();
  $('#extra_links').empty().hide();
  if (mi >= 0) {
    $('#modal_map_index').show();
    $('#modal_map_index .value').text(mi);
  } else {
    $('#modal_map_index').hide();
    $('#modal_map_index .value').text('');
  }
  if (isSubDag) {
    $('#div_btn_subdag').show();
    subdagId = `${dagId}.${t}`;
  } else {
    $('#div_btn_subdag').hide();
    subdagId = undefined;
  }

  // Show a span or dropdown for mapIndex
  if (mi >= 0 && !mapStates.length) {
    $('#modal_map_index').show();
    $('#modal_map_index .value').text(mi);
    $('#mapped_dropdown').hide();
  } else if (mi >= 0 || isMapped) {
    $('#modal_map_index').show();
    $('#modal_map_index .value').text('');
    $('#mapped_dropdown').show();

    const dropdownText = mapIndex > -1 ? mapIndex : `All  ${mapStates.length} Mapped Instances`;
    $('#mapped_dropdown #dropdown-label').text(dropdownText);
    $('#mapped_dropdown .dropdown-menu').empty();
    $('#mapped_dropdown .dropdown-menu').append(`<li><a href="#" class="map_index_item" data-mapIndex="all">All ${mapStates.length} Mapped Instances</a></li>`);
    mapStates.forEach((state, i) => {
      $('#mapped_dropdown .dropdown-menu').append(`<li><a href="#" class="map_index_item" data-mapIndex="${i}">${i} - ${state}</a></li>`);
    });
  } else {
    $('#modal_map_index').hide();
    $('#modal_map_index .value').text('');
    $('#mapped_dropdown').hide();
  }

  if (isMapped) {
    $('#task_actions').text(`Task Actions for all ${mappedStates.length} instances`);
    $('#btn_mapped').show();
    $('#mapped_dropdown').css('display', 'inline-block');
    $('#btn_rendered').hide();
    $('#btn_xcom').hide();
    $('#btn_log').hide();
    $('#btn_task').hide();
  } else {
    $('#task_actions').text('Task Actions');
    $('#btn_rendered').show();
    $('#btn_xcom').show();
    $('#btn_log').show();
    $('#btn_mapped').hide();
    $('#btn_task').show();
  }

  $('#dag_dl_logs').hide();
  $('#dag_redir_logs').hide();
  if (tryNumber > 0 && !isMapped) {
    $('#dag_dl_logs').show();
    if (showExternalLogRedirect) {
      $('#dag_redir_logs').show();
    }
  }

  updateModalUrls();

  $('#try_index > li').remove();
  $('#redir_log_try_index > li').remove();
  const startIndex = (tryNumber > 2 ? 0 : 1);

  const query = new URLSearchParams({
    dag_id: dagId,
    task_id: taskId,
    execution_date: executionDate,
    metadata: 'null',
  });
  if (mi !== undefined) {
    query.set('map_index', mi);
  }
  for (let index = startIndex; index < tryNumber; index += 1) {
    let showLabel = index;
    if (index !== 0) {
      query.set('try_number', index);
    } else {
      showLabel = 'All';
    }

    $('#try_index').append(`<li role="presentation" style="display:inline">
      <a href="${logsWithMetadataUrl}?${query}&format=file"> ${showLabel} </a>
      </li>`);

    if (index !== 0 || showExternalLogRedirect) {
      $('#redir_log_try_index').append(`<li role="presentation" style="display:inline">
      <a href="${externalLogUrl}?${query}"> ${showLabel} </a>
      </li>`);
    }
  }
  query.delete('try_number');

  if (!isMapped && extraLinks && extraLinks.length > 0) {
    const markupArr = [];
    extraLinks.sort();
    $.each(extraLinks, (i, link) => {
      query.set('link_name', link);
      const externalLink = $('<a href="#" class="btn btn-primary disabled"></a>');
      const linkTooltip = $('<span class="tool-tip" data-toggle="tooltip" style="padding-right: 2px; padding-left: 3px" data-placement="top" '
        + 'title="link not yet available"></span>');
      linkTooltip.append(externalLink);
      externalLink.text(link);

      $.ajax(
        {
          url: `${extraLinksUrl}?${query}`,
          cache: false,
          success(data) {
            externalLink.attr('href', data.url);
            // open absolute (external) links in a new tab/window and relative (local) links
            // directly
            if (/^(?:[a-z]+:)?\/\//.test(data.url)) {
              externalLink.attr('target', '_blank');
            }
            externalLink.removeClass('disabled');
            linkTooltip.tooltip('disable');
          },
          error(data) {
            linkTooltip.tooltip('hide').attr('title', data.responseJSON.error).tooltip('fixTitle');
          },
        },
      );

      markupArr.push(linkTooltip);
    });

    const extraLinksSpan = $('#extra_links');
    extraLinksSpan.prev('hr').show();
    extraLinksSpan.append(markupArr).show();
    extraLinksSpan.find('[data-toggle="tooltip"]').tooltip();
  }
}

// Switch the modal from a mapped task summary to a specific mapped task instance
$(document).on('click', '.map_index_item', function mapItem() {
  const mi = $(this).attr('data-mapIndex');
  if (mi === 'all') {
    callModal({
      taskId,
      executionDate,
      dagRunId,
      extraLinks,
      mapIndex: -1,
      isMapped: true,
      mappedStates: mapStates,
    });
  } else {
    callModal({
      taskId,
      executionDate,
      dagRunId,
      extraLinks,
      mapIndex: mi,
    });
  }
});

// Task Instance Modal actions
$('form[data-action]').on('submit', function submit(e) {
  e.preventDefault();
  const form = $(this).get(0);
  if (dagRunId || executionDate) {
    if (form.dag_run_id) {
      form.dag_run_id.value = dagRunId;
    }
    if (form.execution_date) {
      form.execution_date.value = executionDate;
    }
    form.origin.value = window.location;
    if (form.task_id) {
      form.task_id.value = taskId;
    }
    if (form.map_index && mapIndex >= 0) {
      form.map_index.value = mapIndex;
    } else if (form.map_index) {
      form.map_index.remove();
    }
    form.action = $(this).data('action');
    form.submit();
  }
});

$('#pause_resume').on('change', function onChange() {
  const $input = $(this);
  const id = $input.data('dag-id');
  const isPaused = $input.is(':checked');
  const url = `${pausedUrl}?is_paused=${isPaused}&dag_id=${encodeURIComponent(id)}`;
  // Remove focus on element so the tooltip will go away
  $input.trigger('blur');
  $input.removeClass('switch-input--error');

  // dispatch an event that React can listen for
  const event = new CustomEvent('paused', { detail: isPaused });
  document.dispatchEvent(event);

  $.post(url).fail(() => {
    setTimeout(() => {
      $input.prop('checked', !isPaused);
      $input.addClass('switch-input--error');
      event.value = !isPaused;
      document.dispatchEvent(event);
    }, 500);
  });
});

$('#next-run').on('mouseover', () => {
  $('#next-run').attr('data-original-title', () => {
    let newTitle = '';
    if (nextRun.createAfter) {
      newTitle += `<strong>Run After:</strong> ${formatDateTime(nextRun.createAfter)}<br>`;
      newTitle += `Next Run: ${approxTimeFromNow(nextRun.createAfter)}<br><br>`;
    }
    if (nextRun.intervalStart && nextRun.intervalEnd) {
      newTitle += '<strong>Data Interval</strong><br>';
      newTitle += `Start: ${formatDateTime(nextRun.intervalStart)}<br>`;
      newTitle += `End: ${formatDateTime(nextRun.intervalEnd)}`;
    }
    return newTitle;
  });
});

$('.next-dataset-triggered').on('click', (e) => {
  const summary = $(e.target).data('summary');
  openDatasetModal(dagId, summary, nextDatasets, nextDatasetsError);
});
