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

/* global describe, test, expect, beforeAll */

import { renderHook } from '@testing-library/react-hooks';
import useGridData from './useGridData';
import { Wrapper } from '../utils/testUtils';

const pendingGridData = {
  groups: {},
  dag_runs: [
    {
      dag_id: 'example_python_operator',
      run_id: 'manual__2021-11-08T21:14:17.170046+00:00',
      start_date: null,
      end_date: null,
      state: 'queued',
      execution_date: '2021-11-08T21:14:17.170046+00:00',
      data_interval_start: '2021-11-08T21:14:17.170046+00:00',
      data_interval_end: '2021-11-08T21:14:17.170046+00:00',
      run_type: 'manual',
    },
  ],
};

describe('Test useGridData hook', () => {
  beforeAll(() => {
    global.autoRefreshInterval = 5;
  });

  test('data is valid camelcase json', () => {
    global.gridData = JSON.stringify(pendingGridData);

    const { result } = renderHook(() => useGridData(), { wrapper: Wrapper });
    const { data } = result.current;

    expect(typeof data === 'object').toBe(true);
    expect(data.dagRuns).toBeDefined();
    expect(data.dag_runs).toBeUndefined();
  });

  test('Can handle no gridData', () => {
    global.gridData = null;

    const { result } = renderHook(() => useGridData(), { wrapper: Wrapper });
    const { data } = result.current;

    expect(data.dagRuns).toStrictEqual([]);
    expect(data.groups).toStrictEqual({});
  });

  test('Can handle empty gridData object', () => {
    global.gridData = {};

    const { result } = renderHook(() => useGridData(), { wrapper: Wrapper });
    const { data } = result.current;

    expect(data.dagRuns).toStrictEqual([]);
    expect(data.groups).toStrictEqual({});
  });
});
