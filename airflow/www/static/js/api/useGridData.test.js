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

import { areActiveRuns } from './useGridData';

describe('Test areActiveRuns()', () => {
  test('Correctly detects active runs', () => {
    const runs = [
      { runType: 'scheduled', state: 'success' },
      { runType: 'manual', state: 'queued' },
    ];
    expect(areActiveRuns(runs)).toBe(true);
  });

  test('Returns false when all runs are resolved', () => {
    const runs = [
      { runType: 'scheduled', state: 'success' },
      { runType: 'manual', state: 'failed' },
      { runType: 'manual', state: 'not_queued' },
    ];
    const result = areActiveRuns(runs);
    expect(result).toBe(false);
  });

  test('Returns false when filtering runs runtype ["backfill"] and state ["not_queued"]', () => {
    const runs = [
      { runType: 'scheduled', state: 'success' },
      { runType: 'manual', state: 'failed' },
      { runType: 'backfill', state: 'not_queued' },
    ];
    const result = areActiveRuns(runs);
    expect(result).toBe(false);
  });

  test('Returns false when filtering runs runtype ["backfill"] and state ["queued"]', () => {
    const runs = [
      { runType: 'scheduled', state: 'success' },
      { runType: 'manual', state: 'failed' },
      { runType: 'backfill', state: 'queued' },
    ];
    const result = areActiveRuns(runs);
    expect(result).toBe(false);
  });

  [
    { runType: 'manual', state: 'queued', result: true },
    { runType: 'manual', state: 'running', result: true },
    { runType: 'scheduled', state: 'queued', result: true },
    { runType: 'scheduled', state: 'running', result: true },
    { runType: 'backfill', state: 'queued', result: false },
    { runType: 'backfill', state: 'running', result: false },
  ].forEach((conf) => {
    test(`Returns ${conf.result} when filtering runs with runtype ["${conf.runType}"] and state ["${conf.state}"]`, () => {
      const runConf = { ...conf };
      delete runConf.result;
      const runs = [runConf];

      const result = areActiveRuns(runs);
      expect(result).toBe(conf.result);
    });
  });

  test('Returns false when there are no runs', () => {
    const result = areActiveRuns();
    expect(result).toBe(false);
  });
});
