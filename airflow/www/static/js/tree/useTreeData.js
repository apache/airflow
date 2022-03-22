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

/* global treeData, localStorage, autoRefreshInterval, fetch */

import { useDisclosure } from '@chakra-ui/react';
import camelcaseKeys from 'camelcase-keys';
import { useQuery } from 'react-query';

import { getMetaValue } from '../utils';

// dagId comes from dag.html
const dagId = getMetaValue('dag_id');
const treeDataUrl = getMetaValue('tree_data');
const numRuns = getMetaValue('num_runs');
const urlRoot = getMetaValue('root');
const isPaused = getMetaValue('is_paused');
const baseDate = getMetaValue('base_date');
const isSchedulerRunning = getMetaValue('is_scheduler_running');

const autoRefreshKey = 'disabledAutoRefresh';

const areActiveRuns = (runs) => runs.filter((run) => ['queued', 'running', 'scheduled'].includes(run.state)).length > 0;

const formatData = (data) => {
  if (!data || !Object.keys(data).length) {
    return {
      groups: {},
      dagRuns: [],
    };
  }
  let formattedData = data;
  // Convert to json if needed
  if (typeof data === 'string') formattedData = JSON.parse(data);
  // change from pascal to camelcase
  formattedData = camelcaseKeys(formattedData, { deep: true });
  return formattedData;
};

const useTreeData = () => {
  const initialData = formatData(treeData);

  const isRefreshDisabled = JSON.parse(localStorage.getItem(autoRefreshKey));
  const defaultIsOpen = (
    isPaused !== 'True'
    && !isRefreshDisabled
    && areActiveRuns(initialData.dagRuns)
    && isSchedulerRunning === 'True'
  );

  const { isOpen: isRefreshOn, onToggle, onClose } = useDisclosure({ defaultIsOpen });

  const onToggleRefresh = () => {
    if (isRefreshOn) {
      localStorage.setItem(autoRefreshKey, 'true');
    } else {
      localStorage.removeItem(autoRefreshKey);
    }
    onToggle();
  };

  const query = useQuery('treeData', async () => {
    try {
      const root = urlRoot ? `&root=${urlRoot}` : '';
      const base = baseDate ? `&base_date=${baseDate}` : '';
      const resp = await fetch(`${treeDataUrl}?dag_id=${dagId}&num_runs=${numRuns}${root}${base}`);
      if (resp) {
        let newData = await resp.json();
        newData = formatData(newData);
        // turn off auto refresh if there are no active runs
        if (!areActiveRuns(newData.dagRuns)) onClose();
        return newData;
      }
    } catch (e) {
      onClose();
      console.error(e);
    }
    return {
      groups: {},
      dagRuns: [],
    };
  }, {
    // only enabled and refetch if the refresh switch is on
    enabled: isRefreshOn,
    refetchInterval: isRefreshOn && autoRefreshInterval * 1000,
    initialData,
  });

  return {
    ...query,
    isRefreshOn,
    onToggleRefresh,
  };
};

export default useTreeData;
