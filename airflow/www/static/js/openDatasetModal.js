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

/* global $, isoDateToTimeEl, document */

import { getMetaValue } from './utils';

function openDatasetModal(dagId, summary) {
  const datasetsUrl = getMetaValue('datasets_url');
  let nextRunUrl = getMetaValue('next_run_datasets_url');
  $('#datasets_tbody').empty();
  $('#datasets_error').hide();
  $('#datasetNextRunModal').modal({});
  $('#dag_id').text(dagId);
  $('#next_run_summary').text(summary);
  $('#datasets-loading-dots').css('display', 'inline-block');
  if (dagId) {
    if (nextRunUrl.includes('__DAG_ID__')) {
      nextRunUrl = nextRunUrl.replace('__DAG_ID__', dagId);
    }
    $.get(nextRunUrl)
      .done(
        (datasets) => {
          datasets.forEach((d) => {
            const row = document.createElement('tr');

            const uriCell = document.createElement('td');
            const datasetLink = document.createElement('a');
            datasetLink.href = `${datasetsUrl}?dataset_id=${d.id}`;
            datasetLink.innerText = d.uri;
            uriCell.append(datasetLink);

            const timeCell = document.createElement('td');
            if (d.created_at) timeCell.append(isoDateToTimeEl(d.created_at));

            row.append(uriCell);
            row.append(timeCell);
            $('#datasets-loading-dots').hide();
            $('#datasets_tbody').append(row);
          });
        },
      ).fail((response, textStatus, err) => {
        $('#datasets-loading-dots').hide();
        const description = (response.responseJSON && response.responseJSON.error) || 'Something went wrong.';
        $('#datasets_error_msg').text(`${textStatus}: ${err} ${description}`);
        $('#datasets_error').show();
      });
  }
}

export default openDatasetModal;
