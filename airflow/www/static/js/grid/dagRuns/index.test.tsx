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
import { render } from '@testing-library/react';
import moment from 'moment-timezone';

import DagRuns from './index';
import { TableWrapper } from '../utils/testUtils';
import * as useGridDataModule from '../api/useGridData';
import type { DagRun } from '../types';

const datestring = (new Date()).toISOString();
const generateRuns = (length: number): DagRun[] => (
  [...Array(length)].map((_, i) => ({
    runId: `run-${i}`,
    dataIntervalStart: datestring,
    dataIntervalEnd: datestring,
    state: 'success',
    runType: 'manual',
    startDate: '2021-11-08T21:14:19.704433+00:00',
    endDate: '2021-11-08T21:17:13.206426+00:00',
    lastSchedulingDecision: datestring,
    executionDate: datestring,
  }))
);

describe('Test DagRuns', () => {
  test('Durations and manual run arrow render correctly, but without any date ticks', () => {
    const dagRuns: DagRun[] = [
      {
        runId: 'run1',
        dataIntervalStart: datestring,
        dataIntervalEnd: datestring,
        startDate: '2021-11-08T21:14:19.704433+00:00',
        endDate: '2021-11-08T21:17:13.206426+00:00',
        state: 'failed',
        runType: 'scheduled',
        executionDate: '2021-11-08T21:14:19.704433+00:00',
        lastSchedulingDecision: datestring,
      },
      {
        runId: 'run2',
        dataIntervalStart: datestring,
        dataIntervalEnd: datestring,
        state: 'success',
        runType: 'manual',
        startDate: '2021-11-09T00:19:43.023200+00:00',
        endDate: '2021-11-09T00:22:18.607167+00:00',
        executionDate: '2021-11-08T21:14:19.704433+00:00',
        lastSchedulingDecision: datestring,
      },
    ];
    const data = {
      groups: {},
      dagRuns,
    };

    const spy = jest.spyOn(useGridDataModule, 'default').mockImplementation(() => ({
      data,
    } as any));
    const {
      queryAllByTestId, getByText, queryByText,
    } = render(<DagRuns />, { wrapper: TableWrapper });

    expect(queryAllByTestId('run')).toHaveLength(2);
    expect(queryAllByTestId('manual-run')).toHaveLength(1);
    expect(getByText('00:02:53')).toBeInTheDocument();
    expect(getByText('00:01:26')).toBeInTheDocument();
    expect(queryByText(moment.utc(dagRuns[0].executionDate).format('MMM DD, HH:mm'))).toBeNull();

    spy.mockRestore();
  });

  test('Top date ticks appear when there are 4 or more runs', () => {
    const data = {
      groups: {},
      dagRuns: generateRuns(4),
    };
    const spy = jest.spyOn(useGridDataModule, 'default').mockImplementation(() => ({
      data,
    } as any));
    const { getByText } = render(<DagRuns />, { wrapper: TableWrapper });
    expect(getByText(moment.utc(datestring).format('MMM DD, HH:mm'))).toBeInTheDocument();
    spy.mockRestore();
  });

  test('Show 1 date tick when there are less than 14 runs', () => {
    const data = {
      groups: {},
      dagRuns: generateRuns(11),
    };
    const spy = jest.spyOn(useGridDataModule, 'default').mockImplementation(() => ({
      data,
    } as any));
    const { queryAllByText } = render(<DagRuns />, { wrapper: TableWrapper });
    expect(queryAllByText(moment.utc(datestring).format('MMM DD, HH:mm'))).toHaveLength(1);
    spy.mockRestore();
  });

  test('Show 2 date ticks when there are 14+ runs', () => {
    const data = {
      groups: {},
      dagRuns: generateRuns(14),
    };
    const spy = jest.spyOn(useGridDataModule, 'default').mockImplementation(() => ({
      data,
    } as any));
    const { queryAllByText } = render(<DagRuns />, { wrapper: TableWrapper });
    expect(queryAllByText(moment.utc(datestring).format('MMM DD, HH:mm'))).toHaveLength(2);
    spy.mockRestore();
  });

  test('Handles empty data correctly', () => {
    const spy = jest.spyOn(useGridDataModule, 'default').mockImplementation(() => ({
      data: { groups: {}, dagRuns: [] },
    } as any));

    const { queryByTestId } = render(<DagRuns />, { wrapper: TableWrapper });
    expect(queryByTestId('run')).toBeNull();

    spy.mockRestore();
  });
});
