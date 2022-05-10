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

/* global autoRefreshInterval, gridData */

import { useQuery } from 'react-query';
import axios from 'axios';

import { getMetaValue } from '../../utils';
import { useAutoRefresh } from '../context/autorefresh';
import { areActiveRuns, formatData } from '../utils/gridData';
import useErrorToast from '../utils/useErrorToast';
import useFilters, {
  BASE_DATE_PARAM, NUM_RUNS_PARAM, RUN_STATE_PARAM, RUN_TYPE_PARAM, now,
} from '../utils/useFilters';

const DAG_ID_PARAM = 'dag_id';

// dagId comes from dag.html
const dagId = getMetaValue(DAG_ID_PARAM);
const gridDataUrl = getMetaValue('grid_data_url') || '';
const urlRoot = getMetaValue('root');

const emptyData = {
  dagRuns: [],
  groups: {},
};

const useGridData = () => {
  const initialData = formatData(gridData, emptyData);
  const { isRefreshOn, stopRefresh } = useAutoRefresh();
  const errorToast = useErrorToast();
  const {
    filters: {
      baseDate, numRuns, runType, runState,
    },
  } = useFilters();

  return useQuery(['gridData', baseDate, numRuns, runType, runState], async () => {
    try {
      const params = {
        root: urlRoot || undefined,
        [DAG_ID_PARAM]: dagId,
        [BASE_DATE_PARAM]: baseDate === now ? undefined : baseDate,
        [NUM_RUNS_PARAM]: numRuns,
        [RUN_TYPE_PARAM]: runType,
        [RUN_STATE_PARAM]: runState,
      };
      const newData = await axios.get(gridDataUrl, { params });
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
    initialData,
    placeholderData: emptyData,
    // only refetch if the refresh switch is on
    refetchInterval: isRefreshOn && autoRefreshInterval * 1000,
    keepPreviousData: true,
  });
};

export default useGridData;
