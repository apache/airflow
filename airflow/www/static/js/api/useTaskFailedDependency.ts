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

import axios, { AxiosResponse } from "axios";
import { useAutoRefresh } from "src/context/autorefresh";
import type { API } from "src/types";
import { useQuery } from "react-query";
import { getMetaValue } from "../utils";

const taskDependencyURI = getMetaValue("task_dependency_api");
const mappedTaskDependencyURI = getMetaValue("mapped_task_dependency_api");

export default function useTaskFailedDependency({
  dagId,
  taskId,
  runId,
  mapIndex,
}: {
  dagId: string;
  taskId: string;
  runId: string;
  mapIndex?: number | undefined;
}) {
  const { isRefreshOn } = useAutoRefresh();
  return useQuery(
    ["taskFailedDependencies", dagId, taskId, runId, mapIndex],
    async () => {
      const definedMapIndex = mapIndex ?? -1;
      const url = (
        definedMapIndex >= 0 ? mappedTaskDependencyURI : taskDependencyURI
      )
        .replace("_DAG_RUN_ID_", runId)
        .replace(
          "_TASK_ID_/0/dependencies",
          `_TASK_ID_/${mapIndex}/dependencies`
        )
        .replace("_TASK_ID_", taskId);

      const datum = await axios.get<
        AxiosResponse,
        API.TaskInstanceDependencyCollection
      >(url);
      return datum;
    },
    { refetchInterval: isRefreshOn && (autoRefreshInterval || 1) * 1000 }
  );
}
