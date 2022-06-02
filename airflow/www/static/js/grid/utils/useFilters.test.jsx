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

/* global describe, expect, jest, test, moment */
import { act, renderHook } from '@testing-library/react-hooks';

import { RouterWrapper } from './testUtils';

const date = new Date();
date.setMilliseconds(0);
jest.useFakeTimers().setSystemTime(date);

// eslint-disable-next-line import/first
import useFilters from './useFilters';

describe('Test useFilters hook', () => {
  test('Initial values when url does not have query params', async () => {
    const { result } = renderHook(() => useFilters(), { wrapper: RouterWrapper });
    const {
      filters: {
        baseDate,
        numRuns,
        runType,
        runState,
      },
    } = result.current;

    expect(baseDate).toBe(date.toISOString());
    expect(numRuns).toBe(global.defaultDagRunDisplayNumber);
    expect(runType).toBeNull();
    expect(runState).toBeNull();
  });

  test.each([
    { fnName: 'onBaseDateChange', paramName: 'baseDate', paramValue: moment.utc().format() },
    { fnName: 'onNumRunsChange', paramName: 'numRuns', paramValue: '10' },
    { fnName: 'onRunTypeChange', paramName: 'runType', paramValue: 'manual' },
    { fnName: 'onRunStateChange', paramName: 'runState', paramValue: 'success' },
  ])('Test $fnName functions', async ({ fnName, paramName, paramValue }) => {
    const { result } = renderHook(() => useFilters(), { wrapper: RouterWrapper });

    await act(async () => {
      result.current[fnName](paramValue);
    });

    expect(result.current.filters[paramName]).toBe(paramValue);

    // clearFilters
    await act(async () => {
      result.current.clearFilters();
    });

    if (paramName === 'baseDate') {
      expect(result.current.filters[paramName]).toBe(date.toISOString());
    } else if (paramName === 'numRuns') {
      expect(result.current.filters[paramName]).toBe(global.defaultDagRunDisplayNumber);
    } else {
      expect(result.current.filters[paramName]).toBeNull();
    }
  });
});
