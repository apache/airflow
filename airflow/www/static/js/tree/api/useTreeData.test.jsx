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
import React from 'react';
import { renderHook } from '@testing-library/react-hooks';
import { QueryClient, QueryClientProvider } from 'react-query';
import useTreeData from './useTreeData';
import { AutoRefreshProvider } from '../context/autorefresh';

/* global describe, test, expect, jest, beforeAll */

const pendingTreeData = {
  groups: {},
  dagRuns: [
    {
      dagId: 'example_python_operator',
      runId: 'manual__2021-11-08T21:14:17.170046+00:00',
      startDate: null,
      endDate: null,
      state: 'queued',
      dataIntervalStart: '2021-11-08T21:14:17.170046+00:00',
      dataIntervalEnd: '2021-11-08T21:14:17.170046+00:00',
      runType: 'manual',
    },
  ],
};

const Wrapper = ({ children }) => {
  const queryClient = new QueryClient();
  return (
    <AutoRefreshProvider>
      <QueryClientProvider client={queryClient}>
        {children}
      </QueryClientProvider>
    </AutoRefreshProvider>
  );
};

describe('Test useTreeData hook', () => {
  beforeAll(() => {
    global.autoRefreshInterval = 5;
    global.fetch = jest.fn();
  });

  test('data is valid camelcase json', () => {
    global.treeData = pendingTreeData;

    const { result } = renderHook(() => useTreeData(), { wrapper: Wrapper });
    const { data } = result.current;

    expect(typeof data === 'object').toBe(true);
    expect(data.dagRuns).toBeDefined();
  });

  test('Can handle no treeData', () => {
    global.treeData = null;

    const { result } = renderHook(() => useTreeData(), { wrapper: Wrapper });
    const { data } = result.current;

    expect(data.dagRuns).toStrictEqual([]);
    expect(data.groups).toStrictEqual({});
  });

  test('Can handle empty treeData object', () => {
    global.treeData = {};

    const { result } = renderHook(() => useTreeData(), { wrapper: Wrapper });
    const { data } = result.current;

    expect(data.dagRuns).toStrictEqual([]);
    expect(data.groups).toStrictEqual({});
  });
});
