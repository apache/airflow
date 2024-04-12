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

import { useQuery } from "react-query";
import axios, { AxiosResponse } from "axios";

import { getMetaValue } from "src/utils";
import { useAutoRefresh } from "src/context/autorefresh";
import useErrorToast from "src/utils/useErrorToast";
import useFilters, {
  BASE_DATE_PARAM,
  NUM_RUNS_PARAM,
  RUN_STATE_PARAM,
  RUN_TYPE_PARAM,
  now,
  FILTER_DOWNSTREAM_PARAM,
  FILTER_UPSTREAM_PARAM,
  ROOT_PARAM,
} from "src/dag/useFilters";
import type { Task, DagRun, RunOrdering, API } from "src/types";
import { camelCase } from "lodash";
import useSelection from "src/dag/useSelection";

const DAG_ID_PARAM = "dag_id";

// dagId comes from dag.html
const dagId = getMetaValue(DAG_ID_PARAM);
const gridDataUrl = getMetaValue("grid_data_url");

export interface GridData {
  dagRuns: DagRun[];
  groups: Task;
  ordering: RunOrdering;
}

export const emptyGridData: GridData = {
  dagRuns: [],
  groups: {
    id: null,
    label: null,
    instances: [],
  },
  ordering: [],
};

const formatOrdering = (data: GridData) => ({
  ...data,
  ordering: data.ordering.map((o: string) => camelCase(o)) as RunOrdering,
});

export const areActiveRuns = (runs: DagRun[] = []) =>
  runs.filter((run) => ["queued", "running"].includes(run.state)).length > 0;

const useGridData = () => {
  const { isRefreshOn, stopRefresh } = useAutoRefresh();
  const errorToast = useErrorToast();
  const {
    filters: {
      baseDate,
      numRuns,
      runType,
      runState,
      root,
      filterDownstream,
      filterUpstream,
    },
    onBaseDateChange,
  } = useFilters();
  const {
    onSelect,
    selected: { taskId, runId },
  } = useSelection();
  const query = useQuery(
    [
      "gridData",
      baseDate,
      numRuns,
      runType,
      runState,
      root,
      filterUpstream,
      filterDownstream,
      runId,
    ],
    async () => {
      const params = {
        [ROOT_PARAM]: root,
        [FILTER_UPSTREAM_PARAM]: filterUpstream,
        [FILTER_DOWNSTREAM_PARAM]: filterDownstream,
        [DAG_ID_PARAM]: dagId,
        [BASE_DATE_PARAM]: baseDate === now ? undefined : baseDate,
        [NUM_RUNS_PARAM]: numRuns,
        [RUN_TYPE_PARAM]: runType,
        [RUN_STATE_PARAM]: runState,
      };
      const response = await axios.get<AxiosResponse, GridData>(gridDataUrl, {
        params,
      });
      if (runId && !response.dagRuns.find((dr) => dr.runId === runId)) {
        const dagRunUrl = getMetaValue("dag_run_url")
          .replace("__DAG_ID__", dagId)
          .replace("__DAG_RUN_ID__", runId);

        // If the run id cannot be found in the response, try fetching it to see if its real and then adjust the base date filter
        try {
          const selectedRun = await axios.get<AxiosResponse, API.DAGRun>(
            dagRunUrl
          );
          if (selectedRun?.executionDate) {
            onBaseDateChange(selectedRun.executionDate);
          }
          // otherwise the run_id isn't valid and we should unselect it
        } catch (e) {
          onSelect({ taskId });
        }
      }
      // turn off auto refresh if there are no active runs
      if (!areActiveRuns(response.dagRuns)) stopRefresh();
      return response;
    },
    {
      // only refetch if the refresh switch is on
      refetchInterval: isRefreshOn && (autoRefreshInterval || 1) * 1000,
      keepPreviousData: true,
      onError: (error: Error) => {
        stopRefresh();
        errorToast({
          title: "Auto-refresh Error",
          error,
        });
        throw error;
      },
      select: formatOrdering,
    }
  );
  return {
    ...query,
    data: query.data ?? emptyGridData,
  };
};

export default useGridData;
