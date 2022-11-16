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
  runType: 'scheduled' as DagRun['runType'],
  queuedAt: null,
  startDate: null,
  endDate: null,
  lastSchedulingDecision: null,
  externalTrigger: false,
  conf: null,
  confIsJson: false,
  notes: '',
};

describe('Test areActiveRuns()', () => {
  test('Correctly detects active runs', () => {
    const runs: DagRun[] = [
      { state: 'success', ...commonDagRunParams },
      { state: 'queued', ...commonDagRunParams },
    ];
    expect(areActiveRuns(runs)).toBe(true);
  });

  test('Returns false when all runs are resolved', () => {
    const runs: DagRun[] = [
      { state: 'success', ...commonDagRunParams },
      { state: 'failed', ...commonDagRunParams },
      { state: 'failed', ...commonDagRunParams },
    ];
    const result = areActiveRuns(runs);
    expect(result).toBe(false);
  });

  test('Returns false when there are no runs', () => {
    const result = areActiveRuns();
    expect(result).toBe(false);
  });
});
