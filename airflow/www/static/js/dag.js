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

import { getMetaValue } from "./utils";
import { approxTimeFromNow, formatDateTime } from "./datetime_utils";
import { openDatasetModal, getDatasetTooltipInfo } from "./datasetUtils";

const dagId = getMetaValue("dag_id");
const pausedUrl = getMetaValue("paused_url");
// eslint-disable-next-line import/prefer-default-export
export const dagTZ = getMetaValue("dag_timezone");
const datasetsUrl = getMetaValue("datasets_url");
const nextRun = {
  createAfter: getMetaValue("next_dagrun_create_after"),
  intervalStart: getMetaValue("next_dagrun_data_interval_start"),
  intervalEnd: getMetaValue("next_dagrun_data_interval_end"),
};
let nextDatasets = [];
let nextDatasetsError;

const setNextDatasets = (datasets, error) => {
  nextDatasets = datasets;
  nextDatasetsError = error;
};

$(window).on("load", function onLoad() {
  $(`a[href*="${this.location.pathname}"]`).parent().addClass("active");
  $(".never_active").removeClass("active");
  const run = $("#next-dataset-tooltip");
  const singleDatasetUri = $(run).data("uri");
  if (!singleDatasetUri) {
    getDatasetTooltipInfo(dagId, run, setNextDatasets);
  }
});

$("#pause_resume").on("change", function onChange() {
  const $input = $(this);
  const id = $input.data("dag-id");
  const isPaused = $input.is(":checked");
  const requireConfirmation = $input.is("[data-require-confirmation]");
  if (requireConfirmation) {
    const confirmation = window.confirm(
      `Are you sure you want to ${isPaused ? "resume" : "pause"} this DAG?`
    );
    if (!confirmation) {
      $input.prop("checked", !isPaused);
      return;
    }
  }
  const url = `${pausedUrl}?is_paused=${isPaused}&dag_id=${encodeURIComponent(
    id
  )}`;
  // Remove focus on element so the tooltip will go away
  $input.trigger("blur");
  $input.removeClass("switch-input--error");

  // dispatch an event that React can listen for
  const event = new CustomEvent("paused", { detail: isPaused });
  document.dispatchEvent(event);

  $.post(url).fail(() => {
    setTimeout(() => {
      $input.prop("checked", !isPaused);
      $input.addClass("switch-input--error");
      event.value = !isPaused;
      document.dispatchEvent(event);
    }, 500);
  });
});

$("#next-run").on("mouseover", () => {
  $("#next-run").attr("data-original-title", () => {
    let newTitle = "";
    if (nextRun.createAfter) {
      newTitle += `<strong>Run After:</strong> ${formatDateTime(
        nextRun.createAfter
      )}<br>`;
      newTitle += `Next Run: ${approxTimeFromNow(nextRun.createAfter)}<br><br>`;
    }
    if (nextRun.intervalStart && nextRun.intervalEnd) {
      newTitle += "<strong>Data Interval</strong><br>";
      newTitle += `Start: ${formatDateTime(nextRun.intervalStart)}<br>`;
      newTitle += `End: ${formatDateTime(nextRun.intervalEnd)}`;
    }
    return newTitle;
  });
});

$(".next-dataset-triggered").on("click", (e) => {
  const run = $("#next-dataset-tooltip");
  const summary = $(e.target).data("summary");
  const singleDatasetUri = $(run).data("uri");
  if (!singleDatasetUri) {
    openDatasetModal(dagId, summary, nextDatasets, nextDatasetsError);
  } else {
    window.location.href = `${datasetsUrl}?uri=${encodeURIComponent(
      singleDatasetUri
    )}`;
  }
});
