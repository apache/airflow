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

/* global document, window, $, d3, STATE_COLOR, isoDateToTimeEl, autoRefreshInterval,
 localStorage */

import { throttle } from "lodash";
import { getMetaValue } from "./utils";
import tiTooltip from "./task_instances";
import { approxTimeFromNow, formatDateTime } from "./datetime_utils";
import { openDatasetModal, getDatasetTooltipInfo } from "./datasetUtils";

const DAGS_INDEX = getMetaValue("dags_index");
const ENTER_KEY_CODE = 13;
const pausedUrl = getMetaValue("paused_url");
const statusFilter = getMetaValue("status_filter");
const autocompleteUrl = getMetaValue("autocomplete_url");
const graphUrl = getMetaValue("graph_url");
const dagRunUrl = getMetaValue("dag_run_url");
const taskInstanceUrl = getMetaValue("task_instance_url");
const blockedUrl = getMetaValue("blocked_url");
const csrfToken = getMetaValue("csrf_token");
const lastDagRunsUrl = getMetaValue("last_dag_runs_url");
const dagStatsUrl = getMetaValue("dag_stats_url");
const taskStatsUrl = getMetaValue("task_stats_url");
const gridUrl = getMetaValue("grid_url");
const datasetsUrl = getMetaValue("datasets_url");
const nextRunDatasetsSummaryUrl = getMetaValue("next_run_datasets_summary_url");

const nextDatasets = {};
let nextDatasetsError;

const DAG_RUN = "dag-run";
const TASK_INSTANCE = "task-instance";

// auto refresh interval in milliseconds
// (x2 the interval in tree/graph view since this page can take longer to refresh )
const refreshIntervalMs = 2000;

$("#tags_filter").select2({
  placeholder: "Filter DAGs by tag",
  allowClear: true,
});

$("#tags_filter").on("change", (e) => {
  e.preventDefault();
  const query = new URLSearchParams(window.location.search);
  if (e.val.length) {
    if (query.has("tags")) query.delete("tags");
    e.val.forEach((value) => {
      query.append("tags", value);
    });
  } else {
    query.delete("tags");
    query.set("reset_tags", "reset");
  }
  if (query.has("page")) query.delete("page");
  window.location = `${DAGS_INDEX}?${query.toString()}`;
});

$("#tags_form").on("reset", (e) => {
  e.preventDefault();
  const query = new URLSearchParams(window.location.search);
  query.delete("tags");
  if (query.has("page")) query.delete("page");
  query.set("reset_tags", "reset");
  window.location = `${DAGS_INDEX}?${query.toString()}`;
});

$("#dag_query").on("keypress", (e) => {
  // check for key press on ENTER (key code 13) to trigger the search
  if (e.which === ENTER_KEY_CODE) {
    const query = new URLSearchParams(window.location.search);
    query.set("search", e.target.value.trim());
    query.delete("page");
    window.location = `${DAGS_INDEX}?${query.toString()}`;
    e.preventDefault();
  }
});

$("#page_size").on("change", function onPageSizeChange() {
  const pSize = $(this).val();
  window.location = `${DAGS_INDEX}?page_size=${pSize}`;
});

$.each($("[id^=toggle]"), function toggleId() {
  const $input = $(this);
  const dagId = $input.data("dag-id");

  $input.on("change", () => {
    const isPaused = $input.is(":checked");
    const url = `${pausedUrl}?is_paused=${isPaused}&dag_id=${encodeURIComponent(
      dagId
    )}`;
    $input.removeClass("switch-input--error");
    // Remove focus on element so the tooltip will go away
    $input.trigger("blur");
    $.post(url).fail(() => {
      setTimeout(() => {
        $input.prop("checked", !isPaused);
        $input.addClass("switch-input--error");
      }, 500);
    });
  });
});

$(".typeahead").typeahead({
  source(query, callback) {
    return $.ajax(autocompleteUrl, {
      data: {
        query: encodeURIComponent(query),
        status: statusFilter,
      },
      success: callback,
    });
  },
  autoSelect: false,
  afterSelect(value) {
    const query = new URLSearchParams(window.location.search);
    query.set("search", value.name);
    if (value.type === "owner") {
      window.location = `${DAGS_INDEX}?${query}`;
    }
    if (value.type === "dag") {
      window.location = `${gridUrl.replace("__DAG_ID__", value.name)}?${query}`;
    }
  },
});

$("#search_form").on("reset", () => {
  const query = new URLSearchParams(window.location.search);
  query.delete("search");
  query.delete("page");
  window.location = `${DAGS_INDEX}?${query}`;
});

$("#main_content").show(250);
const diameter = 25;
const circleMargin = 4;
const strokeWidth = 2;
const strokeWidthHover = 6;

function blockedHandler(error, json) {
  $.each(json, function handleBlock() {
    const a = document.querySelector(`[data-dag-id="${this.dag_id}"]`);
    a.title = `${this.active_dag_run}/${this.max_active_runs} active dag runs`;
    if (this.active_dag_run >= this.max_active_runs) {
      a.style.color = "#e43921";
    }
  });
}

function lastDagRunsHandler(error, json) {
  $(".js-loading-last-run").remove();
  Object.keys(json).forEach((safeDagId) => {
    const dagId = json[safeDagId].dag_id;
    const executionDate = json[safeDagId].execution_date;
    const g = d3.select(`#last-run-${safeDagId}`);

    // Show last run as a link to the graph view
    g.selectAll("a")
      .attr(
        "href",
        `${graphUrl}?dag_id=${encodeURIComponent(
          dagId
        )}&execution_date=${encodeURIComponent(executionDate)}`
      )
      .html("")
      .insert(isoDateToTimeEl.bind(null, executionDate, { title: false }));

    // Only show the tooltip when we have a last run and add the json to a custom data- attribute
    g.selectAll("span")
      .style("display", null)
      .attr("data-lastrun", JSON.stringify(json[safeDagId]));
  });
}

// Load data-lastrun attribute data to populate the tooltip on hover
d3.selectAll(".js-last-run-tooltip").on(
  "mouseover",
  function mouseoverLastRun() {
    const lastRunData = JSON.parse(d3.select(this).attr("data-lastrun"));
    d3.select(this).attr("data-original-title", tiTooltip(lastRunData));
  }
);

function drawDagStats(selector, dagId, states) {
  const g = d3
    .select(`svg#${selector}-${dagId.replace(/\./g, "__dot__")}`)
    .attr("height", diameter + strokeWidthHover * 2)
    .attr("width", states.length * (diameter + circleMargin) + circleMargin)
    .selectAll("g")
    .data(states)
    .enter()
    .append("g")
    .attr("transform", (d, i) => {
      const x = i * (diameter + circleMargin) + (diameter / 2 + circleMargin);
      const y = diameter / 2 + strokeWidthHover;
      return `translate(${x},${y})`;
    });

  g.append("svg:a")
    .attr("href", (d) => {
      const params = new URLSearchParams();
      params.append("_flt_3_dag_id", dagId);
      /* eslint no-unused-expressions: ["error", { "allowTernary": true }] */
      d.state
        ? params.append("_flt_3_state", d.state)
        : params.append("_flt_8_state", "");
      switch (selector) {
        case DAG_RUN:
          return `${dagRunUrl}?${params.toString()}`;
        case TASK_INSTANCE:
          return `${taskInstanceUrl}?${params.toString()}`;
        default:
          return "";
      }
    })
    .append("circle")
    .attr(
      "id",
      (d) => `${selector}-${dagId.replace(/\./g, "_")}-${d.state || "none"}`
    )
    .attr("class", "has-svg-tooltip")
    .attr("stroke-width", (d) => {
      if (d.count > 0) return strokeWidth;
      return 1;
    })
    .attr("stroke", (d) => {
      if (d.count > 0) return STATE_COLOR[d.state];
      return "gainsboro";
    })
    .attr("fill", "#fff")
    .attr("r", diameter / 2)
    .attr("title", (d) => d.state || "none")
    .on("mouseover", (d) => {
      if (d.count > 0) {
        d3.select(d3.event.currentTarget)
          .transition()
          .duration(400)
          .attr("fill", "#e2e2e2")
          .style("stroke-width", strokeWidthHover);
      }
    })
    .on("mouseout", (d) => {
      if (d.count > 0) {
        d3.select(d3.event.currentTarget)
          .transition()
          .duration(400)
          .attr("fill", "#fff")
          .style("stroke-width", strokeWidth);
      }
    })
    .style("opacity", 0)
    .transition()
    .duration(300)
    .delay((d, i) => i * 50)
    .style("opacity", 1);

  d3.select(`.js-loading-${selector}-stats`).remove();

  g.append("text")
    .attr("fill", "#51504f")
    .attr("text-anchor", "middle")
    .attr("vertical-align", "middle")
    .attr("font-size", 9)
    .attr("y", 3)
    .style("pointer-events", "none")
    .text((d) => (d.count > 0 ? d.count : ""));
}

function dagStatsHandler(selector, json) {
  Object.keys(json).forEach((dagId) => {
    const states = json[dagId];
    drawDagStats(selector, dagId, states);
  });
}

function nextRunDatasetsSummaryHandler(_, json) {
  [...document.getElementsByClassName("next-dataset-triggered")].forEach(
    (el) => {
      const dagId = $(el).attr("data-dag-id");
      const previousSummary = $(el).attr("data-summary");
      const nextDatasetsInfo = json[dagId];

      // Only update dags that depend on multiple datasets
      if (nextDatasetsInfo && !nextDatasetsInfo.uri) {
        const newSummary = `${nextDatasetsInfo.ready} of ${nextDatasetsInfo.total} datasets updated`;

        // Only update the element if the summary has changed
        if (previousSummary !== newSummary) {
          $(el).attr("data-summary", newSummary);
          $(el).text(newSummary);
        }
      }
    }
  );
}

function getDagIds({ activeDagsOnly = false } = {}) {
  let dagIds = $("[id^=toggle]");
  if (activeDagsOnly) {
    dagIds = dagIds.filter(":checked");
  }
  dagIds = dagIds
    .map(function () {
      return $(this).data("dag-id");
    })
    .get();
  return dagIds;
}

function getDagStats() {
  const dagIds = getDagIds();
  const params = new URLSearchParams();
  dagIds.forEach((dagId) => {
    params.append("dag_ids", dagId);
  });
  if (params.has("dag_ids")) {
    d3.json(blockedUrl)
      .header("X-CSRFToken", csrfToken)
      .post(params, blockedHandler);
    d3.json(lastDagRunsUrl)
      .header("X-CSRFToken", csrfToken)
      .post(params, lastDagRunsHandler);
    d3.json(dagStatsUrl)
      .header("X-CSRFToken", csrfToken)
      .post(params, (error, json) => dagStatsHandler(DAG_RUN, json));
    d3.json(taskStatsUrl)
      .header("X-CSRFToken", csrfToken)
      .post(params, (error, json) => dagStatsHandler(TASK_INSTANCE, json));
  } else {
    // no dags, hide the loading dots
    $(`.js-loading-${DAG_RUN}-stats`).remove();
    $(`.js-loading-${TASK_INSTANCE}-stats`).remove();
  }
}

function showSvgTooltip(text, circ) {
  const tip = $("#svg-tooltip");
  tip.children(".tooltip-inner").text(text);
  const centeringOffset = tip.width() / 2;
  tip.css({
    display: "block",
    left: `${circ.left + 12.5 - centeringOffset}px`, // 12.5 == half of circle width
    top: `${circ.top - 25}px`, // 25 == position above circle
  });
}

function hideSvgTooltip() {
  $("#svg-tooltip").css("display", "none");
}

function refreshDagStats(selector, dagId, states) {
  d3.select(`svg#${selector}-${dagId.replace(/\./g, "__dot__")}`)
    .selectAll("circle")
    .data(states)
    .attr("stroke-width", (d) => {
      if (d.count > 0) return strokeWidth;
      return 1;
    })
    .attr("stroke", (d) => {
      if (d.count > 0) return STATE_COLOR[d.state];
      return "gainsboro";
    });

  d3.select(`svg#${selector}-${dagId.replace(/\./g, "__dot__")}`)
    .selectAll("text")
    .data(states)
    .text((d) => {
      if (d.count > 0) {
        return d.count;
      }
      return "";
    });
}

let refreshInterval;

function checkActiveRuns(json) {
  // filter latest dag runs and check if there are still running dags
  const activeRuns = Object.keys(json).filter((dagId) => {
    const dagRuns = json[dagId]
      .filter(({ state }) => state === "running" || state === "queued")
      .filter((r) => r.count > 0);
    return dagRuns.length > 0;
  });
  if (activeRuns.length === 0) {
    // in case there are no active runs increase the interval for auto refresh
    $("#auto_refresh").prop("checked", false);
    clearInterval(refreshInterval);
  }
}

function refreshDagStatsHandler(selector, json) {
  if (selector === DAG_RUN) checkActiveRuns(json);
  Object.keys(json).forEach((dagId) => {
    const states = json[dagId];
    refreshDagStats(selector, dagId, states);
  });
}

function handleRefresh({ activeDagsOnly = false } = {}) {
  const dagIds = getDagIds({ activeDagsOnly });
  const params = new URLSearchParams();
  dagIds.forEach((dagId) => {
    params.append("dag_ids", dagId);
  });
  $("#loading-dots").css("display", "inline-block");
  if (params.has("dag_ids")) {
    d3.json(lastDagRunsUrl)
      .header("X-CSRFToken", csrfToken)
      .post(params, lastDagRunsHandler);
    d3.json(dagStatsUrl)
      .header("X-CSRFToken", csrfToken)
      .post(params, (error, json) => refreshDagStatsHandler(DAG_RUN, json));
    d3.json(taskStatsUrl)
      .header("X-CSRFToken", csrfToken)
      .post(params, (error, json) =>
        refreshDagStatsHandler(TASK_INSTANCE, json)
      );
    d3.json(nextRunDatasetsSummaryUrl)
      .header("X-CSRFToken", csrfToken)
      .post(params, nextRunDatasetsSummaryHandler);
  }
  setTimeout(() => {
    $("#loading-dots").css("display", "none");
  }, refreshIntervalMs);
}

function startOrStopRefresh() {
  if ($("#auto_refresh").is(":checked")) {
    refreshInterval = setInterval(() => {
      handleRefresh({ activeDagsOnly: true });
    }, autoRefreshInterval * refreshIntervalMs);
  } else {
    clearInterval(refreshInterval);
  }
}

function initAutoRefresh() {
  const isDisabled = localStorage.getItem("dagsDisableAutoRefresh");
  $("#auto_refresh").prop("checked", !isDisabled);
  startOrStopRefresh();
  d3.select("#refresh_button").on("click", () => handleRefresh());
}

// pause autorefresh when the page is not active
const handleVisibilityChange = () => {
  if (document.hidden) {
    clearInterval(refreshInterval);
  } else {
    initAutoRefresh();
  }
};

document.addEventListener("visibilitychange", handleVisibilityChange);

$(window).on("load", () => {
  initAutoRefresh();

  $("body").on("mouseover", ".has-svg-tooltip", (e) => {
    const elem = e.target;
    const text = elem.getAttribute("title");
    const circ = elem.getBoundingClientRect();
    showSvgTooltip(text, circ);
  });

  $("body").on("mouseout", ".has-svg-tooltip", () => {
    hideSvgTooltip();
  });

  getDagStats();
});

$(".js-next-run-tooltip").each((i, run) => {
  $(run).on("mouseover", () => {
    $(run).attr("data-original-title", () => {
      const nextRunData = $(run).attr("data-nextrun");
      const [createAfter, intervalStart, intervalEnd] = nextRunData.split(",");
      let newTitle = "";
      newTitle += `<strong>Run After:</strong> ${formatDateTime(
        createAfter
      )}<br>`;
      newTitle += `Next Run: ${approxTimeFromNow(createAfter)}<br><br>`;
      newTitle += "<strong>Data Interval</strong><br>";
      newTitle += `Start: ${formatDateTime(intervalStart)}<br>`;
      newTitle += `End: ${formatDateTime(intervalEnd)}`;
      return newTitle;
    });
  });
});

$("#auto_refresh").change(() => {
  if ($("#auto_refresh").is(":checked")) {
    // Run an initial refresh before starting interval if manually turned on
    handleRefresh({ activeDagsOnly: true });
    localStorage.removeItem("dagsDisableAutoRefresh");
  } else {
    localStorage.setItem("dagsDisableAutoRefresh", "true");
    $("#loading-dots").css("display", "none");
  }
  startOrStopRefresh();
});

$(".next-dataset-triggered").on("click", (e) => {
  const dagId = $(e.target).data("dag-id");
  const summary = $(e.target).data("summary");
  const singleDatasetUri = $(e.target).data("uri");

  // If there are multiple datasets, open a modal, otherwise link directly to the dataset
  if (!singleDatasetUri) {
    if (dagId)
      openDatasetModal(dagId, summary, nextDatasets[dagId], nextDatasetsError);
  } else {
    window.location.href = `${datasetsUrl}?uri=${encodeURIComponent(
      singleDatasetUri
    )}`;
  }
});

const getTooltipInfo = throttle(
  (dagId, run, setNextDatasets) =>
    getDatasetTooltipInfo(dagId, run, setNextDatasets),
  1000
);

$(".js-dataset-triggered").each((i, cell) => {
  $(cell).on("mouseover", () => {
    const run = $(cell).children();
    const dagId = $(run).data("dag-id");
    const singleDatasetUri = $(run).data("uri");

    const setNextDatasets = (datasets, error) => {
      nextDatasets[dagId] = datasets;
      nextDatasetsError = error;
    };

    // Only update the tooltip info if there are multiple datasets
    if (!singleDatasetUri) {
      getTooltipInfo(dagId, run, setNextDatasets);
    }
  });
});
