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
import { Text, VStack } from "@chakra-ui/react";

import type { TriggerDetailResponse } from "openapi/requests/types.gen";

import { RouterLink } from "src/system-components";

import { getTaskInstanceLink } from "src/utils/links";

/** Links to whatever the trigger runs for: its task instance, its callback's Dag run, or its assets. */
export const TriggerOwner = ({ trigger }: { readonly trigger: TriggerDetailResponse }) => {
  const { asset_watchers: watchers, callback, task_instance: ti } = trigger;

  if (ti !== null) {
    return (
      <RouterLink
        to={getTaskInstanceLink({
          dagId: ti.dag_id,
          dagRunId: ti.dag_run_id,
          mapIndex: ti.map_index,
          taskId: ti.task_id,
        })}
      >
        {`${ti.dag_id} / ${ti.task_id}${ti.map_index >= 0 ? ` [${ti.map_index}]` : ""}`}
      </RouterLink>
    );
  }

  if (callback !== null) {
    return callback.dag_id !== null && callback.dag_run_id !== null ? (
      <RouterLink to={`/dags/${callback.dag_id}/runs/${callback.dag_run_id}/callbacks`}>
        {`${callback.dag_id} / ${callback.dag_run_id}`}
      </RouterLink>
    ) : (
      <Text>{callback.dag_id ?? callback.id}</Text>
    );
  }

  return watchers.length ? (
    <VStack align="start" gap={0}>
      {watchers.map((watcher) => (
        <RouterLink key={`${watcher.asset_id}-${watcher.watcher_name}`} to={`/assets/${watcher.asset_id}`}>
          {`${watcher.asset_name} / ${watcher.watcher_name}`}
        </RouterLink>
      ))}
    </VStack>
  ) : undefined;
};
