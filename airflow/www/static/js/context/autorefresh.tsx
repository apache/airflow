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

/* global localStorage, document, autoRefreshInterval */

import React, {
  useMemo,
  useContext,
  useState,
  useEffect,
  useCallback,
  PropsWithChildren,
} from "react";

import { getMetaValue } from "src/utils";

const autoRefreshKey = "disabledAutoRefresh";

const initialIsPaused = getMetaValue("is_paused") === "True";
const isRefreshDisabled = JSON.parse(
  localStorage.getItem(autoRefreshKey) || "false"
);

type RefreshContext = {
  isRefreshOn: boolean;
  isPaused: boolean;
  refetchInterval: number | false;
  toggleRefresh: () => void;
  stopRefresh: () => void;
  startRefresh: () => void;
};

const AutoRefreshContext = React.createContext<RefreshContext>({
  isRefreshOn: false,
  isPaused: true,
  refetchInterval: false,
  toggleRefresh: () => {},
  stopRefresh: () => {},
  startRefresh: () => {},
});

export const AutoRefreshProvider = ({ children }: PropsWithChildren) => {
  const [isPaused, setIsPaused] = useState(initialIsPaused);
  const isRefreshAllowed = !(isPaused || isRefreshDisabled);
  const initialState = isRefreshAllowed;

  const [isRefreshOn, setRefresh] = useState(initialState);

  const onToggle = useCallback(() => setRefresh(!isRefreshOn), [isRefreshOn]);
  const stopRefresh = () => setRefresh(false);

  const startRefresh = useCallback(
    () => isRefreshAllowed && setRefresh(true),
    [isRefreshAllowed, setRefresh]
  );

  const refetchInterval = isRefreshOn && (autoRefreshInterval || 1) * 1000;

  const toggleRefresh = useCallback(
    (updateStorage = false) => {
      if (updateStorage) {
        if (isRefreshOn) {
          localStorage.setItem(autoRefreshKey, "true");
        } else {
          localStorage.removeItem(autoRefreshKey);
        }
        onToggle();
      } else if (isRefreshAllowed) {
        onToggle();
      }
    },
    [isRefreshAllowed, isRefreshOn, onToggle]
  );

  useEffect(() => {
    function isCustomEvent(event: Event): event is CustomEvent {
      return "detail" in event;
    }

    const handleChange = (e: Event) => {
      if (isCustomEvent(e)) {
        setIsPaused(!e.detail);
        if (!e.detail) {
          stopRefresh();
        }
      }
    };

    document.addEventListener("paused", handleChange);
    return () => {
      document.removeEventListener("paused", handleChange);
    };
  });

  const value = useMemo(
    () => ({
      isRefreshOn,
      refetchInterval,
      toggleRefresh,
      stopRefresh,
      startRefresh,
      isPaused,
    }),
    [isPaused, isRefreshOn, startRefresh, toggleRefresh, refetchInterval]
  );

  return (
    <AutoRefreshContext.Provider value={value}>
      {children}
    </AutoRefreshContext.Provider>
  );
};

export const useAutoRefresh = () => useContext(AutoRefreshContext);
