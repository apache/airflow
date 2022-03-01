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

/* global treeData, localStorage, fetch, autoRefreshInterval */

import { useState, useEffect, useCallback } from 'react';
import { useDisclosure } from '@chakra-ui/react';
import camelcaseKeys from 'camelcase-keys';

import { getMetaValue } from '../utils';

// dagId comes from dag.html
const dagId = getMetaValue('dag_id');
const treeDataUrl = getMetaValue('tree_data');
const numRuns = getMetaValue('num_runs');
const urlRoot = getMetaValue('root');
const isPaused = getMetaValue('is_paused');
const baseDate = getMetaValue('base_date');

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
  // change from pacal to camelcase
  formattedData = camelcaseKeys(formattedData, { deep: true });
  return formattedData;
};

const useTreeData = () => {
  const [data, setData] = useState(formatData(treeData));
  const defaultIsOpen = isPaused !== 'True' && !JSON.parse(localStorage.getItem('disableAutoRefresh')) && areActiveRuns(data.dagRuns);
  const { isOpen: isRefreshOn, onToggle, onClose } = useDisclosure({ defaultIsOpen });

  const handleRefresh = useCallback(async () => {
    try {
      const root = urlRoot ? `&root=${urlRoot}` : '';
      const base = baseDate ? `&base_date=${baseDate}` : '';
      const resp = await fetch(`${treeDataUrl}?dag_id=${dagId}&num_runs=${numRuns}${root}${base}`);
      let newData = await resp.json();
      if (newData) {
        newData = formatData(newData);
        if (JSON.stringify(newData) !== JSON.stringify(data)) {
          setData(newData);
        }
        // turn off auto refresh if there are no active runs
        if (!areActiveRuns(newData.dagRuns)) onClose();
      }
    } catch (e) {
      onClose();
      console.error(e);
    }
  }, [data, onClose]);

  const onToggleRefresh = () => {
    if (isRefreshOn) {
      localStorage.setItem('disableAutoRefresh', 'true');
    } else {
      localStorage.removeItem('disableAutoRefresh');
    }
    onToggle();
  };

  useEffect(() => {
    let refreshInterval;
    if (isRefreshOn) {
      refreshInterval = setInterval(handleRefresh, autoRefreshInterval * 1000);
    } else {
      clearInterval(refreshInterval);
    }
    return () => clearInterval(refreshInterval);
  }, [isRefreshOn, handleRefresh]);

  return {
    data,
    isRefreshOn,
    onToggleRefresh,
  };
};

export default useTreeData;
