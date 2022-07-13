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

import { LogLevel, parseLogs } from './utils';

const mockTaskLog = `
5d28cfda3219
*** Reading local file: /root/airflow/logs/dag_id=test_ui_grid/run_id=scheduled__2022-06-03T00:00:00+00:00/task_id=section_1.get_entry_group/attempt=1.log
[2022-06-04 00:00:01,901] {taskinstance.py:1132} INFO - Dependencies all met for <TaskInstance: test_ui_grid.section_1.get_entry_group scheduled__2022-06-03T00:00:00+00:00 [queued]>
[2022-06-04 00:00:01,906] {taskinstance.py:1132} INFO - Dependencies all met for <TaskInstance: test_ui_grid.section_1.get_entry_group scheduled__2022-06-03T00:00:00+00:00 [queued]>
[2022-06-04 00:00:01,906] {taskinstance.py:1329} INFO -
--------------------------------------------------------------------------------
[2022-06-04 00:00:01,906] {taskinstance.py:1330} INFO - Starting attempt 1 of 1
[2022-06-04 00:00:01,906] {taskinstance.py:1331} INFO -
--------------------------------------------------------------------------------
[2022-06-04 00:00:01,916] {taskinstance.py:1350} INFO - Executing <Task(BashOperator): section_1.get_entry_group> on 2022-06-03 00:00:00+00:00
[2022-06-04 00:00:01,919] {standard_task_runner.py:52} INFO - Started process 41646 to run task
[2022-06-04 00:00:01,920] {standard_task_runner.py:80} INFO - Running: ['***', 'tasks', 'run', 'test_ui_grid', 'section_1.get_entry_group', 'scheduled__2022-06-03T00:00:00+00:00', '--job-id', '1626', '--raw', '--subdir', 'DAGS_FOLDER/test_ui_grid.py', '--cfg-path', '/tmp/tmpte7k80ur']
[2022-06-04 00:00:01,921] {standard_task_runner.py:81} INFO - Job 1626: Subtask section_1.get_entry_group
[2022-06-04 00:00:01,921] {dagbag.py:507} INFO - Filling up the DagBag from /files/dags/test_ui_grid.py
[2022-06-04 00:00:01,964] {task_command.py:377} INFO - Running <TaskInstance: test_ui_grid.section_1.get_entry_group scheduled__2022-06-03T00:00:00+00:00 [running]> on host 5d28cfda3219
[2022-06-04 00:00:02,010] {taskinstance.py:1548} WARNING - Exporting the following env vars:
AIRFLOW_CTX_DAG_OWNER=***
AIRFLOW_CTX_DAG_ID=test_ui_grid
`;

describe('Test Logs Utils.', () => {
  test('parseLogs function replaces datetimes', () => {
    const { parsedLogs, fileSources } = parseLogs(
      mockTaskLog,
      'UTC',
      [],
      [],
    );

    expect(parsedLogs).toContain('2022-06-04, 00:00:01 UTC');
    expect(fileSources).toEqual([
      'dagbag.py',
      'standard_task_runner.py',
      'task_command.py',
      'taskinstance.py',
    ]);
    const result = parseLogs(
      mockTaskLog,
      'America/Los_Angeles',
      [],
      [],
    );
    expect(result.parsedLogs).toContain('2022-06-03, 17:00:01 PDT');
  });

  test.each([
    { logLevelFilters: [LogLevel.INFO], expectedNumberOfLines: 11, expectedNumberOfFileSources: 4 },
    {
      logLevelFilters: [LogLevel.WARNING],
      expectedNumberOfLines: 1,
      expectedNumberOfFileSources: 1,
    },
  ])(
    'Filtering logs on $logLevelFilters level should return $expectedNumberOfLines lines and $expectedNumberOfFileSources file sources',
    ({
      logLevelFilters,
      expectedNumberOfLines, expectedNumberOfFileSources,
    }) => {
      const { parsedLogs, fileSources } = parseLogs(
        mockTaskLog,
        null,
        logLevelFilters,
        [],
      );

      expect(fileSources).toHaveLength(expectedNumberOfFileSources);
      expect(parsedLogs).toBeDefined();
      const lines = parsedLogs!.split('\n');
      expect(lines).toHaveLength(expectedNumberOfLines);
      lines.forEach((line) => expect(line).toContain(logLevelFilters[0]));
    },
  );

  test('parseLogs function with file source filter', () => {
    const { parsedLogs, fileSources } = parseLogs(
      mockTaskLog,
      null,
      [],
      ['taskinstance.py'],
    );

    expect(fileSources).toEqual([
      'dagbag.py',
      'standard_task_runner.py',
      'task_command.py',
      'taskinstance.py',
    ]);
    const lines = parsedLogs!.split('\n');
    expect(lines).toHaveLength(7);
    lines.forEach((line) => expect(line).toContain('taskinstance.py'));
  });

  test('parseLogs function with filter on log level and file source', () => {
    const { parsedLogs, fileSources } = parseLogs(
      mockTaskLog,
      null,
      [LogLevel.INFO, LogLevel.WARNING],
      ['taskinstance.py'],
    );

    expect(fileSources).toEqual([
      'dagbag.py',
      'standard_task_runner.py',
      'task_command.py',
      'taskinstance.py',
    ]);
    const lines = parsedLogs!.split('\n');
    expect(lines).toHaveLength(7);
    lines.forEach((line) => expect(line).toMatch(/INFO|WARNING/));
  });
});
