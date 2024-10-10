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

import { getMetaValue } from "./utils";

export function openDatasetModal(dagId, summary, nextDatasets, error) {
  const assetEvents = nextDatasets.events || [];
  const expression = nextDatasets.dataset_expression;
  const datasetsUrl = getMetaValue("datasets_url");
  $("#dataset_expression").empty();
  $("#datasets_tbody").empty();
  $("#datasets_error").hide();
  $("#dag_id").text(dagId);
  $("#dataset_expression").text(JSON.stringify(expression, null, 2));
  $("#datasetNextRunModal").modal({});
  if (summary) $("#next_run_summary").text(summary);
  assetEvents.forEach((d) => {
    const row = document.createElement("tr");

    const uriCell = document.createElement("td");
    const datasetLink = document.createElement("a");
    datasetLink.href = `${datasetsUrl}?uri=${encodeURIComponent(d.uri)}`;
    datasetLink.innerText = d.uri;
    uriCell.append(datasetLink);

    const timeCell = document.createElement("td");
    if (d.lastUpdate) timeCell.append(isoDateToTimeEl(d.lastUpdate));

    row.append(uriCell);
    row.append(timeCell);
    $("#datasets_tbody").append(row);
  });

  if (error) {
    $("#datasets_error_msg").text(error);
    $("#datasets_error").show();
  }
}

export function getDatasetTooltipInfo(dagId, run, setNextDatasets) {
  let nextRunUrl = getMetaValue("next_run_datasets_url");
  if (dagId) {
    if (nextRunUrl.includes("__DAG_ID__")) {
      nextRunUrl = nextRunUrl.replace("__DAG_ID__", dagId);
    }
    $.get(nextRunUrl)
      .done((nextDatasets) => {
        const assetEvents = nextDatasets.events;
        let count = 0;
        let title = "<strong>Pending datasets:</strong><br>";
        setNextDatasets(nextDatasets);
        assetEvents.forEach((d) => {
          if (!d.created_at) {
            if (count < 4) title += `${d.uri}<br>`;
            count += 1;
          }
        });
        if (count > 4) {
          title += `<br>And ${count - 4} more.`;
        }
        title += "<br>Click to see more details.";
        $(run).attr("data-original-title", () => title);
      })
      .fail((response, textStatus, err) => {
        const description =
          (response.responseJSON && response.responseJSON.error) ||
          "Something went wrong.";
        const error = `${textStatus}: ${err} ${description}`;
        setNextDatasets([], error);
      });
  }
}
