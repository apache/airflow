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

import React, {
  useContext, useReducer, useEffect, useRef,
} from 'react';
import { useSearchParams } from 'react-router-dom';

const SelectionContext = React.createContext(null);

const SELECT = 'SELECT';
const DESELECT = 'DESELECT';
const RUN_ID = 'dag_run_id';
const TASK_ID = 'task_id';

const selectionReducer = (state, { type, payload }) => {
  switch (type) {
    case SELECT:
      // Deselect if it is the same selection
      if (payload.taskId === state.taskId && payload.runId === state.runId) {
        return {};
      }
      return payload;
    case DESELECT:
      return {};
    default:
      return state;
  }
};

// Expose the grid selection to any react component instead of passing around lots of props
export const SelectionProvider = ({ children }) => {
  const [searchParams, setSearchParams] = useSearchParams();
  const [selected, dispatch] = useReducer(selectionReducer, {});

  const rendered = useRef(false);

  // Clear selection, but keep other search params
  const clearSelection = () => {
    searchParams.delete(RUN_ID);
    searchParams.delete(TASK_ID);
    setSearchParams(searchParams);
    dispatch({ type: DESELECT });
  };

  const onSelect = (payload) => {
    const params = new URLSearchParams(searchParams);
    if (payload.runId) params.set(RUN_ID, payload.runId);
    if (payload.taskId) params.set(TASK_ID, payload.taskId);
    setSearchParams(params);
    dispatch({ type: SELECT, payload });
  };

  // Check search params and set selection but only on the first render
  useEffect(() => {
    if (!rendered.current) {
      const runId = searchParams.get(RUN_ID);
      const taskId = searchParams.get(TASK_ID);
      if (runId) {
        dispatch({ type: SELECT, payload: { runId, taskId } });
      }
      rendered.current = true;
    }
  }, [searchParams]);

  return (
    <SelectionContext.Provider value={{ selected, clearSelection, onSelect }}>
      {children}
    </SelectionContext.Provider>
  );
};

export const useSelection = () => useContext(SelectionContext);
