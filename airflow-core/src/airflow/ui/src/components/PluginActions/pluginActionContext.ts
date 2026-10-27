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
import type { PluginProps } from "src/pages/ReactPlugin";

import type { AppliesToContext } from "src/utils/pluginAppliesTo";
import type { PluginTemplateTokens } from "src/utils/pluginTemplate";

import type { PluginActionEntity } from "./types";

type PluginActionContext = {
  /** The record half of the scoping context; the route supplies the rest. */
  appliesTo: Pick<AppliesToContext, "dag" | "dagRun" | "taskInstance">;
  pluginProps: PluginProps;
  tokens: PluginTemplateTokens;
};

/**
 * Derive everything a surface needs about its entity: the scoping context, the props a React
 * action's component receives, and the tokens a link action's URL interpolates.
 *
 * One pure function for all six surfaces -- three table rows and three header cards -- so they
 * cannot drift on what a plugin is told about the row it was clicked from. Deliberately not a
 * hook: a table calls this per row, where mounting query observers would be ruinous.
 *
 * Each case supplies the *whole* record it was handed, not a stub, so an `applies_to` path can
 * address any field the REST API returns for it -- `state` being the one that motivated this.
 * A record the surface does not have stays `undefined`, which is what makes paths rooted at it
 * skipped rather than failed.
 */
export const pluginActionContext = (entity: PluginActionEntity): PluginActionContext => {
  if (entity.destination === "dag") {
    const { dag, dagDetails } = entity;

    return {
      appliesTo: { dag },
      pluginProps: { dag: dagDetails, dagId: dag.dag_id },
      tokens: { dagId: dag.dag_id },
    };
  }

  if (entity.destination === "dag_run") {
    const { dagRun } = entity;

    return {
      appliesTo: { dagRun },
      pluginProps: { dagId: dagRun.dag_id, dagRun, runId: dagRun.dag_run_id },
      tokens: { dagId: dagRun.dag_id, runId: dagRun.dag_run_id },
    };
  }

  // Narrowed to the task instance case. Adding a destination to the union breaks this
  // destructuring rather than silently falling through, which is the exhaustiveness check.
  const { taskInstance } = entity;
  // Stringified rather than omitted when -1, so `{MAP_INDEX}` resolves for unmapped tasks
  // instead of being left in the URL verbatim.
  const mapIndex = String(taskInstance.map_index);
  // Rows for different loop regions of one task share every other identifier, so without these
  // a link action would give them all the same URL. Omitted rather than stringified when
  // absent, which leaves `{REGION_ID}` in the URL so a half-resolved link fails visibly.
  const regionId = taskInstance.region_id ?? undefined;
  const regionIndex = taskInstance.region_index ?? undefined;

  return {
    appliesTo: { taskInstance },
    pluginProps: {
      dagId: taskInstance.dag_id,
      mapIndex,
      runId: taskInstance.dag_run_id,
      taskId: taskInstance.task_id,
      taskInstance,
    },
    tokens: {
      dagId: taskInstance.dag_id,
      mapIndex,
      regionId,
      regionIndex: regionIndex === undefined ? undefined : String(regionIndex),
      runId: taskInstance.dag_run_id,
      taskId: taskInstance.task_id,
    },
  };
};
