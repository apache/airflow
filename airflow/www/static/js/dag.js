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

/* global document, window, $, */

import getMetaValue from './meta_value';

function updateQueryStringParameter(uri, key, value) {
  const re = new RegExp(`([?&])${key}=.*?(&|$)`, 'i');
  const separator = uri.indexOf('?') !== -1 ? '&' : '?';
  if (uri.match(re)) {
    return uri.replace(re, `$1${key}=${value}$2`);
  }

  return `${uri}${separator}${key}=${value}`;
}

// Pills highlighting
$(window).on('load', function onLoad() {
  $(`a[href*="${this.location.pathname}"]`).parent().addClass('active');
  $('.never_active').removeClass('active');
});

const dagId = getMetaValue('dag_id');
export const dagTZ = getMetaValue('dag_timezone');
const logsWithMetadataUrl = getMetaValue('logs_with_metadata_url');
const externalLogUrl = getMetaValue('external_log_url');
const extraLinksUrl = getMetaValue('extra_links_url');
const pausedUrl = getMetaValue('paused_url');
let taskId = '';
let executionDate = '';
let subdagId = '';
const showExternalLogRedirect = getMetaValue('show_external_log_redirect') === 'True';

const buttons = Array.from(document.querySelectorAll('a[id^="btn_"][data-base-url]')).reduce((obj, elm) => {
  obj[elm.id.replace('btn_', '')] = elm;
  return obj;
}, {});

function updateButtonUrl(elm, params) {
  elm.setAttribute('href', `${elm.dataset.baseUrl}?${$.param(params)}`);
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
  });

  updateButtonUrl(buttons.rendered, {
    dag_id: dagId,
    task_id: taskId,
    execution_date: executionDate,
  });

  if (buttons.rendered_k8s) {
    updateButtonUrl(buttons.rendered_k8s, {
      dag_id: dagId,
      task_id: taskId,
      execution_date: executionDate,
    });
  }

  updateButtonUrl(buttons.ti, {
    flt1_dag_id_equals: dagId,
    _flt_3_task_id: taskId,
    _oc_TaskInstanceModelView: executionDate,
  });

  updateButtonUrl(buttons.log, {
    dag_id: dagId,
    task_id: taskId,
    execution_date: executionDate,
  });
}

// Update modal urls on toggle
document.addEventListener('click', (event) => {
  if (event.target.matches('button[data-toggle="button"]')) {
    updateModalUrls();
  }
});

export function callModal(t, d, extraLinks, tryNumbers, sd) {
  taskId = t;
  const location = String(window.location);
  $('#btn_filter').on('click', () => {
    window.location = updateQueryStringParameter(location, 'root', taskId);
  });
  subdagId = sd;
  executionDate = d;
  $('#task_id').text(t);
  $('#execution_date').text(d);
  $('#taskInstanceModal').modal({});
  $('#taskInstanceModal').css('margin-top', '0');
  $('#extra_links').prev('hr').hide();
  $('#extra_links').empty().hide();
  if (subdagId === undefined) $('#div_btn_subdag').hide();
  else {
    $('#div_btn_subdag').show();
    subdagId = `${dagId}.${t}`;
  }

  $('#dag_dl_logs').hide();
  $('#dag_redir_logs').hide();
  if (tryNumbers > 0) {
    $('#dag_dl_logs').show();
    if (showExternalLogRedirect) {
      $('#dag_redir_logs').show();
    }
  }

  updateModalUrls();

  $('#try_index > li').remove();
  $('#redir_log_try_index > li').remove();
  const startIndex = (tryNumbers > 2 ? 0 : 1);
  for (let index = startIndex; index < tryNumbers; index += 1) {
    let url = `${logsWithMetadataUrl
    }?dag_id=${encodeURIComponent(dagId)
    }&task_id=${encodeURIComponent(taskId)
    }&execution_date=${encodeURIComponent(executionDate)
    }&metadata=null`
      + '&format=file';

    let showLabel = index;
    if (index !== 0) {
      url += `&try_number=${index}`;
    } else {
      showLabel = 'All';
    }

    $('#try_index').append(`<li role="presentation" style="display:inline">
      <a href="${url}"> ${showLabel} </a>
      </li>`);

    if (index !== 0 || showExternalLogRedirect) {
      const redirLogUrl = `${externalLogUrl
      }?dag_id=${encodeURIComponent(dagId)
      }&task_id=${encodeURIComponent(taskId)
      }&execution_date=${encodeURIComponent(executionDate)
      }&try_number=${index}`;
      $('#redir_log_try_index').append(`<li role="presentation" style="display:inline">
      <a href="${redirLogUrl}"> ${showLabel} </a>
      </li>`);
    }
  }

  if (extraLinks && extraLinks.length > 0) {
    const markupArr = [];
    extraLinks.sort();
    $.each(extraLinks, (i, link) => {
      const url = `${extraLinksUrl
      }?task_id=${encodeURIComponent(taskId)
      }&dag_id=${encodeURIComponent(dagId)
      }&execution_date=${encodeURIComponent(executionDate)
      }&link_name=${encodeURIComponent(link)}`;
      const externalLink = $('<a href="#" class="btn btn-primary disabled" target="_blank"></a>');
      const linkTooltip = $('<span class="tool-tip" data-toggle="tooltip" style="padding-right: 2px; padding-left: 3px" data-placement="top" '
        + 'title="link not yet available"></span>');
      linkTooltip.append(externalLink);
      externalLink.text(link);

      $.ajax(
        {
          url,
          cache: false,
          success(data) {
            externalLink.attr('href', data.url);
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

export function callModalDag(dag) {
  $('#dagModal').modal({});
  $('#dagModal').css('margin-top', '0');
  executionDate = dag.execution_date;
  updateButtonUrl(buttons.dag_graph_view, {
    dag_id: dag && dag.dag_id,
    execution_date: dag && dag.execution_date,
  });
}

// Task Instance Modal actions
$('form[data-action]').on('submit', function submit(e) {
  e.preventDefault();
  const form = $(this).get(0);
  // Somehow submit is fired twice. Only once is the executionDate valid
  if (executionDate) {
    form.execution_date.value = executionDate;
    form.origin.value = window.location;
    if (form.task_id) {
      form.task_id.value = taskId;
    }
    form.action = $(this).data('action');
    form.submit();
  }
});

// DAG Modal actions
$('form button[data-action]').on('click', function onClick() {
  const form = $(this).closest('form').get(0);
  // Somehow submit is fired twice. Only once is the executionDate valid
  if (executionDate) {
    form.execution_date.value = executionDate;
    form.origin.value = window.location;
    if (form.task_id) {
      form.task_id.value = taskId;
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
  $input.removeClass('switch-input--error');
  $.post(url).fail(() => {
    setTimeout(() => {
      $input.prop('checked', !isPaused);
      $input.addClass('switch-input--error');
    }, 500);
  });
});
