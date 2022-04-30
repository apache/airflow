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

/* global autoRefreshInterval */

import { useQuery } from 'react-query';
import axios from 'axios';

import { getMetaValue } from '../../utils';
import { useAutoRefresh } from '../context/autorefresh';
import { areActiveRuns } from '../utils/gridData';
import useErrorToast from '../utils/useErrorToast';
import useFilters from '../context/filters';

// dagId comes from dag.html
const dagId = getMetaValue('dag_id');
const gridDataUrl = getMetaValue('grid_data_url') || '';
const urlRoot = getMetaValue('root');

const useGridData = () => {
  const { isRefreshOn, stopRefresh } = useAutoRefresh();
  const errorToast = useErrorToast();
  const {
    filters: {
      baseDate, numRuns, runType, runState,
    },
  } = useFilters();

  return useQuery(['useGridData', baseDate, numRuns, runType, runState], async () => {
    try {
      const rootParam = urlRoot ? `&root=${urlRoot}` : '';
      const baseDateParam = baseDate ? `&base_date=${baseDate}` : '';
      const numRunsParam = numRuns ? `&num_runs=${numRuns}` : '';
      const runTypeParam = runType ? `&run_type=${runType}` : '';
      const runStateParam = runState ? `&run_state=${runState}` : '';
      const newData = await axios.get(`${gridDataUrl}?dag_id=${dagId}${numRunsParam}${rootParam}${baseDateParam}${runTypeParam}${runStateParam}`);
      // turn off auto refresh if there are no active runs
      if (!areActiveRuns(newData.dagRuns)) stopRefresh();
      return newData;
    } catch (error) {
      stopRefresh();
      errorToast({
        title: 'Auto-refresh Error',
        error,
      });
      throw (error);
    }
  }, {
    // only refetch if the refresh switch is on
    refetchInterval: isRefreshOn && autoRefreshInterval * 1000,
  });
};

export default useGridData;
