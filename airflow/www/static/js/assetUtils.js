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

export function openAssetModal(dagId, summary, nextAssets, error) {
  const assetEvents = nextAssets.events || [];
  const expression = nextAssets.asset_expression;
  const assetsUrl = getMetaValue("assets_url");
  $("#asset_expression").empty();
  $("#assets_tbody").empty();
  $("#assets_error").hide();
  $("#dag_id").text(dagId);
  $("#asset_expression").text(JSON.stringify(expression, null, 2));
  $("#assetNextRunModal").modal({});
  if (summary) $("#next_run_summary").text(summary);
  assetEvents.forEach((d) => {
    const row = document.createElement("tr");

    const uriCell = document.createElement("td");
    const assetLink = document.createElement("a");
    assetLink.href = `${assetsUrl}?uri=${encodeURIComponent(d.uri)}`;
    assetLink.innerText = d.uri;
    uriCell.append(assetLink);

    const timeCell = document.createElement("td");
    if (d.lastUpdate) timeCell.append(isoDateToTimeEl(d.lastUpdate));

    row.append(uriCell);
    row.append(timeCell);
    $("#assets_tbody").append(row);
  });

  if (error) {
    $("#assets_error_msg").text(error);
    $("#assets_error").show();
  }
}

export function getAssetTooltipInfo(dagId, run, setNextAssets) {
  let nextRunUrl = getMetaValue("next_run_assets_url");
  if (dagId) {
    if (nextRunUrl.includes("__DAG_ID__")) {
      nextRunUrl = nextRunUrl.replace("__DAG_ID__", dagId);
    }
    $.get(nextRunUrl)
      .done((nextAssets) => {
        const assetEvents = nextAssets.events;
        let count = 0;
        let title = "<strong>Pending assets:</strong><br>";
        setNextAssets(nextAssets);
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
        setNextAssets([], error);
      });
  }
}
