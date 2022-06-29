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
import { renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from 'react-query';
import nock from 'nock';

import useTasks from './useTasks';
import * as metaUtils from '../../utils';

const Wrapper = ({ children }) => {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: 0,
      },
    },
  });
  return (
    <QueryClientProvider client={queryClient}>
      {children}
    </QueryClientProvider>
  );
};

const fakeUrl = 'http://fake.api';

describe('Test useTasks hook', () => {
  let spy;
  beforeEach(() => {
    spy = jest.spyOn(metaUtils, 'getMetaValue').mockReturnValue(`${fakeUrl}`);
  });

  afterEach(() => {
    spy.mockRestore();
    nock.cleanAll();
  });

  test('initialData works normally', async () => {
    const scope = nock(fakeUrl)
      .get('/')
      .reply(200, { totalEntries: 1, tasks: [{ taskId: 'task_id' }] });
    const { result } = renderHook(() => useTasks(), { wrapper: Wrapper });

    expect(result.current.data.totalEntries).toBe(0);
    expect(result.current.isFetching).toBeTruthy();

    await waitFor(() => expect(result.current.isFetching).toBeFalsy());

    expect(result.current.data.totalEntries).toBe(1);
    scope.done();
  });

  test('initialData persists even if there is an error', async () => {
    const scope = nock(fakeUrl)
      .get('/')
      .replyWithError('something awful happened');
    const { result } = renderHook(() => useTasks(), { wrapper: Wrapper });

    expect(result.current.data.totalEntries).toBe(0);

    await waitFor(() => expect(result.current.isError).toBeTruthy());

    expect(result.current.data.totalEntries).toBe(0);
    scope.done();
  });
});
