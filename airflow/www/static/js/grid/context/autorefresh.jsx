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

/* global localStorage, document */

import React, {
  useMemo, useContext, useState, useEffect, useCallback,
} from 'react';
import { getMetaValue } from '../../utils';

const autoRefreshKey = 'disabledAutoRefresh';

const initialIsPaused = getMetaValue('is_paused') === 'True';
const isRefreshDisabled = JSON.parse(localStorage.getItem(autoRefreshKey));

const AutoRefreshContext = React.createContext({
  isRefreshOn: false,
  isPaused: true,
  toggleRefresh: () => {},
  stopRefresh: () => {},
  startRefresh: () => {},
});

export const AutoRefreshProvider = ({ children }) => {
  const [isPaused, setIsPaused] = useState(initialIsPaused);
  const isRefreshAllowed = !(isPaused || isRefreshDisabled);
  const initialState = isRefreshAllowed;

  const [isRefreshOn, setRefresh] = useState(initialState);

  const onToggle = useCallback(
    () => setRefresh(!isRefreshOn),
    [isRefreshOn],
  );
  const stopRefresh = () => setRefresh(false);

  const startRefresh = useCallback(
    () => isRefreshAllowed && setRefresh(true),
    [isRefreshAllowed, setRefresh],
  );

  const toggleRefresh = useCallback(
    (updateStorage = false) => {
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
    },
    [isRefreshAllowed, isRefreshOn, onToggle],
  );

  useEffect(() => {
    const handleChange = (e) => {
      setIsPaused(!e.detail);
      if (!e.detail) {
        stopRefresh();
      }
    };

    document.addEventListener('paused', handleChange);
    return () => {
      document.removeEventListener('paused', handleChange);
    };
  });

  const value = useMemo(() => ({
    isRefreshOn, toggleRefresh, stopRefresh, startRefresh, isPaused,
  }), [isPaused, isRefreshOn, startRefresh, toggleRefresh]);

  return (
    <AutoRefreshContext.Provider value={value}>
      {children}
    </AutoRefreshContext.Provider>
  );
};

export const useAutoRefresh = () => useContext(AutoRefreshContext);
