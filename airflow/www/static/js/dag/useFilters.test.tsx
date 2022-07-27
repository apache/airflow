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
import { act, renderHook } from '@testing-library/react';

import { RouterWrapper } from 'src/utils/testUtils';

declare global {
  namespace NodeJS {
    interface Global {
      defaultDagRunDisplayNumber: number;
    }
  }
}

const date = new Date();
date.setMilliseconds(0);
jest.useFakeTimers().setSystemTime(date);

// eslint-disable-next-line import/first
import useFilters, { FilterHookReturn, Filters, UtilFunctions } from './useFilters';

describe('Test useFilters hook', () => {
  test('Initial values when url does not have query params', async () => {
    const { result } = renderHook<FilterHookReturn, undefined>(
      () => useFilters(),
      { wrapper: RouterWrapper },
    );
    const {
      filters: {
        baseDate,
        numRuns,
        runType,
        runState,
      },
    } = result.current;

    expect(baseDate).toBe(date.toISOString());
    expect(numRuns).toBe(global.defaultDagRunDisplayNumber.toString());
    expect(runType).toBeNull();
    expect(runState).toBeNull();
  });

  test.each([
    { fnName: 'onBaseDateChange' as keyof UtilFunctions, paramName: 'baseDate' as keyof Filters, paramValue: moment.utc().format() },
    { fnName: 'onNumRunsChange' as keyof UtilFunctions, paramName: 'numRuns' as keyof Filters, paramValue: '10' },
    { fnName: 'onRunTypeChange' as keyof UtilFunctions, paramName: 'runType' as keyof Filters, paramValue: 'manual' },
    { fnName: 'onRunStateChange' as keyof UtilFunctions, paramName: 'runState' as keyof Filters, paramValue: 'success' },
  ])('Test $fnName functions', async ({ fnName, paramName, paramValue }) => {
    const { result } = renderHook<FilterHookReturn, undefined>(
      () => useFilters(),
      { wrapper: RouterWrapper },
    );

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
      expect(result.current.filters[paramName]).toBe(global.defaultDagRunDisplayNumber.toString());
    } else {
      expect(result.current.filters[paramName]).toBeNull();
    }
  });
});
