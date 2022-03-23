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

/* global localStorage, treeData */

import React, { useContext, useState } from 'react';
import { getMetaValue } from '../../utils';
import { formatData, areActiveRuns } from '../treeDataUtils';

const autoRefreshKey = 'disabledAutoRefresh';

const isPaused = getMetaValue('is_paused') === 'True';
const isRefreshDisabled = JSON.parse(localStorage.getItem(autoRefreshKey));
const isRefreshAllowed = !(isPaused || isRefreshDisabled);

const AutoRefreshContext = React.createContext(null);

export const AutoRefreshProvider = ({ children }) => {
  const dagRuns = treeData && treeData.dag_runs ? formatData(treeData.dag_runs, []) : [];
  const isActive = areActiveRuns(dagRuns);
  const initialState = isRefreshAllowed && isActive;

  const [isRefreshOn, setRefresh] = useState(initialState);

  const onToggle = () => setRefresh(!isRefreshOn);
  const stopRefresh = () => setRefresh(false);
  const startRefresh = () => isRefreshAllowed && setRefresh(true);

  const toggleRefresh = (updateStorage = false) => {
    if (updateStorage) {
      if (isRefreshOn) {
        localStorage.setItem(autoRefreshKey, 'true');
      } else {
        localStorage.removeItem(autoRefreshKey);
      }
      onToggle();
    } else if (isRefreshAllowed) {
      onToggle();
    }
  };

  return (
    <AutoRefreshContext.Provider
      value={{
        isRefreshOn, toggleRefresh, stopRefresh, startRefresh,
      }}
    >
      {children}
    </AutoRefreshContext.Provider>
  );
};

export const useAutoRefresh = () => useContext(AutoRefreshContext);
