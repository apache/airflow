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

import { useQuery } from 'react-query';
import axios, { AxiosResponse } from 'axios';

import { getMetaValue } from 'src/utils';
import { useAutoRefresh } from 'src/context/autorefresh';
import useErrorToast from 'src/utils/useErrorToast';
import useFilters, {
  BASE_DATE_PARAM, NUM_RUNS_PARAM, RUN_STATE_PARAM, RUN_TYPE_PARAM, now,
} from 'src/dag/useFilters';
import type { Task, DagRun, RunOrdering } from 'src/types';
import { camelCase } from 'lodash';

const DAG_ID_PARAM = 'dag_id';

// dagId comes from dag.html
const dagId = getMetaValue(DAG_ID_PARAM);
const gridDataUrl = getMetaValue('grid_data_url');
const urlRoot = getMetaValue('root');

export interface GridData {
  dagRuns: DagRun[];
  groups: Task;
  ordering: RunOrdering;
}

const emptyGridData: GridData = {
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

export const areActiveRuns = (runs: DagRun[] = []) => runs.filter((run) => ['manual', 'scheduled', 'dataset_triggered'].includes(run.runType)).filter((run) => ['queued', 'running'].includes(run.state)).length > 0;

const useGridData = () => {
  const { isRefreshOn, stopRefresh } = useAutoRefresh();
  const errorToast = useErrorToast();
  const {
    filters: {
      baseDate, numRuns, runType, runState,
    },
  } = useFilters();

  const query = useQuery(
    ['gridData', baseDate, numRuns, runType, runState],
    async () => {
      const params = {
        root: urlRoot || undefined,
        [DAG_ID_PARAM]: dagId,
        [BASE_DATE_PARAM]: baseDate === now ? undefined : baseDate,
        [NUM_RUNS_PARAM]: numRuns,
        [RUN_TYPE_PARAM]: runType,
        [RUN_STATE_PARAM]: runState,
      };
      const response = await axios.get<AxiosResponse, GridData>(gridDataUrl, { params });
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
          title: 'Auto-refresh Error',
          error,
        });
        throw (error);
      },
      select: formatOrdering,
    },
  );
  return {
    ...query,
    data: query.data ?? emptyGridData,
  };
};

export default useGridData;
