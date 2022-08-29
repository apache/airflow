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

import type { DagRun } from 'src/types';
import { areActiveRuns } from './useGridData';

const commonDagRunParams = {
  runId: 'runId',
  executionDate: '2022-01-01T10:00+00:00',
  dataIntervalStart: '2022-01-01T05:00+00:00',
  dataIntervalEnd: '2022-01-01T10:00+00:00',
  startDate: null,
  endDate: null,
  lastSchedulingDecision: null,
};

describe('Test areActiveRuns()', () => {
  test('Correctly detects active runs', () => {
    const runs: DagRun[] = [
      { runType: 'scheduled', state: 'success', ...commonDagRunParams },
      { runType: 'manual', state: 'queued', ...commonDagRunParams },
    ];
    expect(areActiveRuns(runs)).toBe(true);
  });

  test('Returns false when all runs are resolved', () => {
    const runs: DagRun[] = [
      { runType: 'scheduled', state: 'success', ...commonDagRunParams },
      { runType: 'manual', state: 'failed', ...commonDagRunParams },
      { runType: 'manual', state: 'failed', ...commonDagRunParams },
    ];
    const result = areActiveRuns(runs);
    expect(result).toBe(false);
  });

  test('Returns false when filtering runs runtype ["backfill"]', () => {
    const runs: DagRun[] = [
      { runType: 'scheduled', state: 'success', ...commonDagRunParams },
      { runType: 'manual', state: 'failed', ...commonDagRunParams },
      { runType: 'backfill', state: 'failed', ...commonDagRunParams },
    ];
    const result = areActiveRuns(runs);
    expect(result).toBe(false);
  });

  test('Returns false when filtering runs runtype ["backfill"] and state ["queued"]', () => {
    const runs: DagRun[] = [
      { runType: 'scheduled', state: 'success', ...commonDagRunParams },
      { runType: 'manual', state: 'failed', ...commonDagRunParams },
      { runType: 'backfill', state: 'queued', ...commonDagRunParams },
    ];
    const result = areActiveRuns(runs);
    expect(result).toBe(false);
  });

  [
    {
      runType: 'manual', state: 'queued', expectedResult: true,
    },
    {
      runType: 'manual', state: 'running', expectedResult: true,
    },
    {
      runType: 'scheduled', state: 'queued', expectedResult: true,
    },
    {
      runType: 'scheduled', state: 'running', expectedResult: true,
    },
    {
      runType: 'dataset_triggered', state: 'queued', expectedResult: true,
    },
    {
      runType: 'dataset_triggered', state: 'running', expectedResult: true,
    },
    {
      runType: 'backfill', state: 'queued', expectedResult: false,
    },
    {
      runType: 'backfill', state: 'running', expectedResult: false,
    },
  ].forEach(({ state, runType, expectedResult }) => {
    test(`Returns ${expectedResult} when filtering runs with runtype ["${runType}"] and state ["${state}"]`, () => {
      const runs: DagRun[] = [{ runType, state, ...commonDagRunParams } as DagRun];
      const result = areActiveRuns(runs);
      expect(result).toBe(expectedResult);
    });
  });

  test('Returns false when there are no runs', () => {
    const result = areActiveRuns();
    expect(result).toBe(false);
  });
});
