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

/* global treeData, autoRefreshInterval */

import { useQuery } from 'react-query';
import axios from 'axios';

import { getMetaValue } from '../../utils';
import { useAutoRefresh } from '../context/autorefresh';
import { formatData, areActiveRuns } from '../utils/treeData';
import useErrorToast from '../utils/useErrorToast';

// dagId comes from dag.html
const dagId = getMetaValue('dag_id');
const treeDataUrl = getMetaValue('tree_data_url') || '';
const numRuns = getMetaValue('num_runs');
const urlRoot = getMetaValue('root');
const baseDate = getMetaValue('base_date');

const emptyData = {
  dagRuns: [],
  groups: {},
};

const useTreeData = () => {
  const initialData = formatData(treeData, emptyData);
  const { isRefreshOn, stopRefresh } = useAutoRefresh();
  const errorToast = useErrorToast();
  return useQuery('treeData', async () => {
    try {
      const params = new URLSearchParams({
        dag_id: dagId,
      });
      if (numRuns && numRuns !== 25) params.append('num_runs', numRuns);
      if (urlRoot) params.append('root', urlRoot);
      if (baseDate) params.append('base_date', baseDate);

      const newData = await axios.get(treeDataUrl, { params });
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
    initialData,
    placeholderData: emptyData,
  });
};

export default useTreeData;
