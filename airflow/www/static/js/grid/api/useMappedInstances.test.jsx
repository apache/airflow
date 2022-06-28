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

/* global describe, test, expect, beforeEach, afterEach, jest */

import React from 'react';
import { renderHook } from '@testing-library/react-hooks';
import { QueryClient, QueryClientProvider } from 'react-query';
import nock from 'nock';

import useMappedInstances from './useMappedInstances';
import { AutoRefreshProvider } from '../context/autorefresh';
import * as metaUtils from '../../utils';

const Wrapper = ({ children }) => {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        initialDataUpdatedAt: new Date().setMinutes(-1),
      },
    },
  });
  return (
    <QueryClientProvider client={queryClient}>
      <AutoRefreshProvider>
        {children}
      </AutoRefreshProvider>
    </QueryClientProvider>
  );
};

const fakeUrl = 'http://fake.api';

describe('Test useMappedInstances hook', () => {
  let spy;
  beforeEach(() => {
    global.autoRefreshInterval = 0.1;
    spy = jest.spyOn(metaUtils, 'getMetaValue').mockReturnValue(`${fakeUrl}/_DAG_RUN_ID_/_TASK_ID_`);
  });

  afterEach(() => {
    spy.mockRestore();
    nock.cleanAll();
  });

  test('autorefresh works normally', async () => {
    const scope = nock(fakeUrl)
      .get('/run_id/task_id')
      .query(true)
      .reply(200, { totalEntries: 1, taskInstances: [{ taskId: 'task_id', state: 'queued' }] });
    const { result, waitFor } = renderHook(() => useMappedInstances({
      dagId: 'dag_id',
      runId: 'run_id',
      taskId: 'task_id',
    }), { wrapper: Wrapper });

    await new Promise((r) => { setTimeout(r, 10); });
    await waitFor(() => result.current.isSuccess);
    scope.done();
    expect(result.current.data.taskInstances[0].state).toBe('queued');

    const scope2 = nock(fakeUrl)
      .get('/run_id/task_id')
      .query(true)
      .reply(200, { totalEntries: 2, taskInstances: [{ taskId: 'task_id', state: 'failed' }] });

    await new Promise((_) => { setTimeout(_, 300); });
    await waitFor(() => result.current.isSuccess);
    scope2.done();
    expect(result.current.data.taskInstances[0].state).toBe('failed');
  });

  test('autorefresh stops if all states are final', async () => {
    const scope = nock(fakeUrl)
      .get('/run_id/task_id')
      .query(true)
      .reply(200, { totalEntries: 1, taskInstances: [{ taskId: 'task_id', state: 'success' }] });
    const { result, waitFor } = renderHook(() => useMappedInstances({
      dagId: 'dag_id',
      runId: 'run_id',
      taskId: 'task_id',
    }), { wrapper: Wrapper });

    await new Promise((r) => { setTimeout(r, 10); });
    await waitFor(() => result.current.isSuccess);
    scope.done();
    expect(result.current.data.taskInstances[0].state).toBe('success');

    const scope2 = nock(fakeUrl)
      .get('/run_id/task_id')
      .query(true)
      .reply(200, { totalEntries: 2, taskInstances: [{ taskId: 'task_id', state: 'failed' }] });

    await new Promise((r) => { setTimeout(r, 300); });
    await waitFor(() => result.current.isSuccess);
    expect(result.current.data.taskInstances[0].state).toBe('success');

    scope2.pendingMocks();
  });
});
