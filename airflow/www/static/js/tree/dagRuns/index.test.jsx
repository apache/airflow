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
import { render } from '@testing-library/react';
import { ChakraProvider, Table, Tbody } from '@chakra-ui/react';
import moment from 'moment-timezone';
import { QueryClient, QueryClientProvider } from 'react-query';
import { MemoryRouter } from 'react-router-dom';

import DagRuns from './index';
import { ContainerRefProvider } from '../context/containerRef';
import { TimezoneProvider } from '../context/timezone';
import { AutoRefreshProvider } from '../context/autorefresh';

global.moment = moment;

const Wrapper = ({ children }) => {
  const queryClient = new QueryClient();
  return (
    <React.StrictMode>
      <ChakraProvider>
        <QueryClientProvider client={queryClient}>
          <ContainerRefProvider value={{}}>
            <TimezoneProvider value={{ timezone: 'UTC' }}>
              <AutoRefreshProvider value={{ isRefreshOn: false, stopRefresh: () => {} }}>
                <MemoryRouter>
                  <Table>
                    <Tbody>
                      {children}
                    </Tbody>
                  </Table>
                </MemoryRouter>
              </AutoRefreshProvider>
            </TimezoneProvider>
          </ContainerRefProvider>
        </QueryClientProvider>
      </ChakraProvider>
    </React.StrictMode>
  );
};

describe('Test DagRuns', () => {
  const dagRuns = [
    {
      dagId: 'dagId',
      runId: 'run1',
      dataIntervalStart: new Date(),
      dataIntervalEnd: new Date(),
      startDate: '2021-11-08T21:14:19.704433+00:00',
      endDate: '2021-11-08T21:17:13.206426+00:00',
      state: 'failed',
      runType: 'scheduled',
      executionDate: '2021-11-08T21:14:19.704433+00:00',
    },
    {
      dagId: 'dagId',
      runId: 'run2',
      dataIntervalStart: new Date(),
      dataIntervalEnd: new Date(),
      state: 'success',
      runType: 'manual',
      startDate: '2021-11-09T00:19:43.023200+00:00',
      endDate: '2021-11-09T00:22:18.607167+00:00',
    },
  ];

  test('Durations and manual run arrow render correctly, but without any date ticks', () => {
    global.treeData = JSON.stringify({
      groups: {},
      dagRuns,
    });
    const { queryAllByTestId, getByText, queryByText } = render(
      <DagRuns />, { wrapper: Wrapper },
    );
    expect(queryAllByTestId('run')).toHaveLength(2);
    expect(queryAllByTestId('manual-run')).toHaveLength(1);

    expect(getByText('00:02:53')).toBeInTheDocument();
    expect(getByText('00:01:26')).toBeInTheDocument();
    expect(queryByText(moment.utc(dagRuns[0].executionDate).format('MMM DD, HH:mm'))).toBeNull();
  });

  test('Top date ticks appear when there are 4 or more runs', () => {
    global.treeData = JSON.stringify({
      groups: {},
      dagRuns: [
        ...dagRuns,
        {
          dagId: 'dagId',
          runId: 'run3',
          dataIntervalStart: new Date(),
          dataIntervalEnd: new Date(),
          startDate: '2021-11-08T21:14:19.704433+00:00',
          endDate: '2021-11-08T21:17:13.206426+00:00',
          state: 'failed',
          runType: 'scheduled',
        },
        {
          dagId: 'dagId',
          runId: 'run4',
          dataIntervalStart: new Date(),
          dataIntervalEnd: new Date(),
          state: 'success',
          runType: 'manual',
          startDate: '2021-11-09T00:19:43.023200+00:00',
          endDate: '2021-11-09T00:22:18.607167+00:00',
        },
      ],
    });
    const { getByText } = render(
      <DagRuns />, { wrapper: Wrapper },
    );
    expect(getByText(moment.utc(dagRuns[0].executionDate).format('MMM DD, HH:mm'))).toBeInTheDocument();
  });

  test('Handles empty data correctly', () => {
    global.treeData = {
      groups: {},
      dagRuns: [],
    };
    const { queryByTestId } = render(
      <DagRuns />, { wrapper: Wrapper },
    );
    expect(queryByTestId('run')).toBeNull();
  });

  test('Handles no data correctly', () => {
    global.treeData = {};
    const { queryByTestId } = render(
      <DagRuns />, { wrapper: Wrapper },
    );
    expect(queryByTestId('run')).toBeNull();
  });
});
