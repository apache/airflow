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

/* global describe, test, expect */

import React from 'react';
import { act, renderHook } from '@testing-library/react-hooks';
import { MemoryRouter } from 'react-router-dom';

import useFilters from './useFilters';

const Wrapper = ({ children }) => (
  <MemoryRouter>
    {children}
  </MemoryRouter>
);

describe('Test useFilters hook', () => {
  test('Initial values when url does not have query params', async () => {
    const { result } = renderHook(() => useFilters(), { wrapper: Wrapper });
    const {
      filters: {
        baseDate,
        numRuns,
        runType,
        runState,
        taskState,
      },
    } = result.current;

    expect(baseDate).toBeNull();
    expect(numRuns).toBeNull();
    expect(runType).toBeNull();
    expect(runState).toBeNull();
    expect(taskState).toBeNull();
  });

  test.each([
    { fnName: 'onBaseDateChange', paramName: 'baseDate', paramValue: new Date().toISOString() },
    { fnName: 'onNumRunsChange', paramName: 'numRuns', paramValue: '10' },
    { fnName: 'onRunTypeChange', paramName: 'runType', paramValue: 'manual' },
    { fnName: 'onRunStateChange', paramName: 'runState', paramValue: 'success' },
    { fnName: 'onTaskStateChange', paramName: 'taskState', paramValue: 'deferred' },
  ])('Test $fnName functions', async ({ fnName, paramName, paramValue }) => {
    const { result } = renderHook(() => useFilters(), { wrapper: Wrapper });

    await act(async () => {
      result.current[fnName]({ target: { value: paramValue } });
    });

    expect(result.current.filters[paramName]).toBe(paramValue);

    // clearFilters
    await act(async () => {
      result.current.clearFilters();
    });

    expect(result.current.filters[paramName]).toBeNull();
  });
});
