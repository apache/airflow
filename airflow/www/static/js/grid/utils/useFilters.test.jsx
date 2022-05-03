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

/* global describe, test, expect, jest */

import React from 'react';
import { act, renderHook } from '@testing-library/react-hooks';
import { MemoryRouter } from 'react-router-dom';

import useFilters from './useFilters';

const Wrapper = ({ children }) => (
  <MemoryRouter>
    {children}
  </MemoryRouter>
);

jest.useFakeTimers().setSystemTime(new Date('2020-01-01'));

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

    expect(baseDate).toBe(new Date('2020-01-01').toISOString().replace('Z', ''));
    expect(numRuns).toBe(25);
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

    if (paramName === 'baseDate') {
      expect(result.current.filters[paramName]).toBe(new Date('2020-01-01').toISOString().replace('Z', ''));
    } else if (paramName === 'numRuns') {
      expect(result.current.filters[paramName]).toBe(25);
    } else {
      expect(result.current.filters[paramName]).toBeNull();
    }
  });
});
