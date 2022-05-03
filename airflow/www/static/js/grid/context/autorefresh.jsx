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

/* global localStorage, gridData, document */

import React, { useContext, useState, useEffect } from 'react';
import { getMetaValue } from '../../utils';
import { formatData, areActiveRuns } from '../utils/gridData';

const autoRefreshKey = 'disabledAutoRefresh';

const initialIsPaused = getMetaValue('is_paused') === 'True';
const isRefreshDisabled = JSON.parse(localStorage.getItem(autoRefreshKey));

const AutoRefreshContext = React.createContext(null);

export const AutoRefreshProvider = ({ children }) => {
  let dagRuns = [];
  try {
    const data = JSON.parse(gridData);
    if (data.dag_runs) dagRuns = formatData(data.dag_runs);
  } catch {
    dagRuns = [];
  }
  const [isPaused, setIsPaused] = useState(initialIsPaused);
  const isActive = areActiveRuns(dagRuns);
  const isRefreshAllowed = !(isPaused || isRefreshDisabled);
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

  useEffect(() => {
    const handleChange = (e) => {
      setIsPaused(!e.value);
      if (!e.value) {
        stopRefresh();
      } else if (isActive) {
        setRefresh(true);
      }
    };

    document.addEventListener('paused', handleChange);
    return () => {
      document.removeEventListener('paused', handleChange);
    };
  });

  return (
    <AutoRefreshContext.Provider
      value={{
        isRefreshOn, toggleRefresh, stopRefresh, startRefresh, isPaused,
      }}
    >
      {children}
    </AutoRefreshContext.Provider>
  );
};

export const useAutoRefresh = () => useContext(AutoRefreshContext);
