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

import { AnsiUp } from "ansi_up";
import { LogLevel, parseLogs } from "./utils";

const mockTaskLogInfoBegin = `5d28cfda3219
*** Reading local file: /root/airflow/logs/dag_id=test_ui_grid/run_id=scheduled__2022-06-03T00:00:00+00:00/task_id=section_1.get_entry_group/attempt=1.log
[2022-06-04 00:00:01,901] {taskinstance.py:1132} INFO - Dependencies all met for <TaskInstance: test_ui_grid.section_1.get_entry_group scheduled__2022-06-03T00:00:00+00:00 [queued]>
[2022-06-04 00:00:01,906] {taskinstance.py:1132} INFO - Dependencies all met for <TaskInstance: test_ui_grid.section_1.get_entry_group scheduled__2022-06-03T00:00:00+00:00 [queued]>
[2022-06-04 00:00:01,906] {taskinstance.py:1329} INFO -
[2022-06-04 00:00:01,906] {taskinstance.py:1330} INFO - Starting attempt 1 of 1
[2022-06-04 00:00:01,906] {taskinstance.py:1331} INFO -
`;
const mockTaskLogErrorWithTraceback = `[2022-06-04 00:00:01,910] {taskinstance.py:3311} ERROR - Task failed with exception
Traceback (most recent call last):
  File "/opt/airflow/airflow/models/taskinstance.py", line 767, in _execute_task
    result = _execute_callable(context=context, **execute_callable_kwargs)
  File "/opt/airflow/airflow/models/taskinstance.py", line 733, in _execute_callable
    return ExecutionCallableRunner(
  File "/opt/airflow/airflow/utils/operator_helpers.py", line 252, in run
    return self.func(*args, **kwargs)
  File "/opt/airflow/airflow/models/baseoperator.py", line 422, in wrapper
    return func(self, *args, **kwargs)
  File "/opt/airflow/airflow/operators/python.py", line 505, in execute
    return super().execute(context=serializable_context)
  File "/opt/airflow/airflow/models/baseoperator.py", line 422, in wrapper
    return func(self, *args, **kwargs)
  File "/opt/airflow/airflow/operators/python.py", line 238, in execute
    return_value = self.execute_callable()
  File "/opt/airflow/airflow/operators/python.py", line 870, in execute_callable
    result = self._execute_python_callable_in_subprocess(python_path)
  File "/opt/airflow/airflow/operators/python.py", line 588, in _execute_python_callable_in_subprocess
    raise AirflowException(error_msg) from None
airflow.exceptions.AirflowException: Process returned non-zero exit status 1.
This is log line 1
This is log line 2
This is log line 3
This is log line 4
This is log line 5
`;
const mockTaskLogWarning = `[2022-06-04 00:00:02,010] {taskinstance.py:1548} WARNING - Exporting env vars: AIRFLOW_CTX_DAG_OWNER=*** AIRFLOW_CTX_DAG_ID=test_ui_grid`;
const mockTaskLogInfoEndWithWarningAndUrl = `[2022-06-04 00:00:01,914] {taskinstance.py:1225} INFO - Marking task as FAILED. dag_id=reproduce_log_error_dag, task_id=reproduce_log_error_python_task2, run_id=manual__2024-11-30T02:18:22.203608+00:00, execution_date=20241130T021822, start_date=20241130T021842, end_date=20241130T021844
[2022-06-04 00:00:01,919] {standard_task_runner.py:52} INFO - Started process 41646 to run task
[2022-06-04 00:00:01,920] {standard_task_runner.py:80} INFO - Running: ['***', 'tasks', 'run', 'test_ui_grid', 'section_1.get_entry_group', 'scheduled__2022-06-03T00:00:00+00:00', '--job-id', '1626', '--raw', '--subdir', 'DAGS_FOLDER/test_ui_grid.py', '--cfg-path', '/tmp/tmpte7k80ur']
[2022-06-04 00:00:01,921] {standard_task_runner.py:81} INFO - Job 1626: Subtask section_1.get_entry_group
[2022-06-04 00:00:01,921] {dagbag.py:507} INFO - Filling up the DagBag from /files/dags/test_ui_grid.py
[2022-06-04 00:00:01,964] {task_command.py:377} INFO - Running <TaskInstance: test_ui_grid.section_1.get_entry_group scheduled__2022-06-03T00:00:00+00:00 [running]> on host 5d28cfda3219
${mockTaskLogWarning}
[2024-07-01 00:00:02,010] {taskinstance.py:1548} INFO - Url parsing test => "https://apple.com", "https://google.com", https://something.logs/_dashboard/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:now-1d,to:now))&_a=(columns:!(_source),filters:!(('$state':(store:appState))))`;

const mockTaskLog = `${mockTaskLogInfoBegin}${mockTaskLogErrorWithTraceback}${mockTaskLogInfoEndWithWarningAndUrl}`;
const ansiUp = new AnsiUp();
const parseExpectedLogs = (logs: string) => {
  ansiUp.url_allowlist = {};
  return logs.split("\n").map((line) => ansiUp.ansi_to_html(line));
};

describe("Test Logs Utils.", () => {
  test("parseLogs function replaces datetimes", () => {
    const { parsedLogs, fileSources } = parseLogs(
      mockTaskLog,
      "UTC",
      [],
      [],
      []
    );

    expect(parsedLogs).toContain("2022-06-04, 00:00:01 UTC");
    expect(fileSources).toEqual([
      "dagbag.py",
      "standard_task_runner.py",
      "task_command.py",
      "taskinstance.py",
    ]);
    const result = parseLogs(mockTaskLog, "America/Los_Angeles", [], [], []);
    expect(result.parsedLogs).toContain("2022-06-03, 17:00:01 PDT");
  });

  test.each([
    {
      logLevelFilters: [LogLevel.INFO],
      expectedNumberOfLines: 14,
      expectedNumberOfFileSources: 4,
      expectedLogs: `${mockTaskLogInfoBegin}${mockTaskLogInfoEndWithWarningAndUrl.replace(
        mockTaskLogWarning,
        ""
      )}`,
    },
    {
      logLevelFilters: [LogLevel.WARNING],
      expectedNumberOfLines: 1,
      expectedNumberOfFileSources: 1,
      expectedLogs: mockTaskLogWarning,
    },
  ])(
    "Filtering logs on $logLevelFilters level should return $expectedNumberOfLines lines and $expectedNumberOfFileSources file sources",
    ({
      logLevelFilters,
      expectedNumberOfLines,
      expectedNumberOfFileSources,
      expectedLogs,
    }) => {
      const { parsedLogs, fileSources } = parseLogs(
        mockTaskLog,
        null,
        logLevelFilters,
        [],
        []
      );

      expect(fileSources).toHaveLength(expectedNumberOfFileSources);
      expect(parsedLogs).toBeDefined();
      const lines = parsedLogs!.split("\n");
      const expectedLines = parseExpectedLogs(expectedLogs);
      expect(lines).toHaveLength(expectedNumberOfLines);
      lines.forEach((line, index) => {
        expect(line).toContain(expectedLines[index]);
      });
    }
  );

  test("parseLogs function with file source filter", () => {
    const { parsedLogs, fileSources } = parseLogs(
      mockTaskLog,
      null,
      [],
      ["taskinstance.py"],
      []
    );
    const expectedLogs = `[2022-06-04 00:00:01,901] {taskinstance.py:1132} INFO - Dependencies all met for <TaskInstance: test_ui_grid.section_1.get_entry_group scheduled__2022-06-03T00:00:00+00:00 [queued]>
[2022-06-04 00:00:01,906] {taskinstance.py:1132} INFO - Dependencies all met for <TaskInstance: test_ui_grid.section_1.get_entry_group scheduled__2022-06-03T00:00:00+00:00 [queued]>
[2022-06-04 00:00:01,906] {taskinstance.py:1329} INFO -
[2022-06-04 00:00:01,906] {taskinstance.py:1330} INFO - Starting attempt 1 of 1
[2022-06-04 00:00:01,906] {taskinstance.py:1331} INFO -
${mockTaskLogErrorWithTraceback}
${mockTaskLogWarning}
[2024-07-01 00:00:02,010] {taskinstance.py:1548} INFO -`; // Ignore matching for transformed hyperlinks; only verify that all the correct lines are returned.

    expect(fileSources).toEqual([
      "dagbag.py",
      "standard_task_runner.py",
      "task_command.py",
      "taskinstance.py",
    ]);
    const lines = parsedLogs!.split("\n");
    const expectedLines = parseExpectedLogs(expectedLogs);
    expect(lines).toHaveLength(34);
    lines.forEach((line, index) => {
      expect(line).toContain(expectedLines[index]);
    });
  });

  test("parseLogs function with filter on log level and file source", () => {
    const { parsedLogs, fileSources } = parseLogs(
      mockTaskLog,
      null,
      [LogLevel.INFO, LogLevel.WARNING],
      ["taskinstance.py"],
      []
    );

    expect(fileSources).toEqual([
      "dagbag.py",
      "standard_task_runner.py",
      "task_command.py",
      "taskinstance.py",
    ]);
    const lines = parsedLogs!.split("\n");
    expect(lines).toHaveLength(8);
    lines.forEach((line) => expect(line).toMatch(/INFO|WARNING/));
  });

  test("parseLogs function with urls", () => {
    const { parsedLogs } = parseLogs(
      mockTaskLog,
      null,
      [LogLevel.INFO, LogLevel.WARNING],
      ["taskinstance.py"],
      []
    );

    // remove the last line which is empty
    const lines = parsedLogs!.split("\n").filter((line) => line.length > 0);
    expect(lines[lines.length - 1]).toContain(
      '<a href="https://apple.com" target="_blank" rel="noopener noreferrer" style="color: blue; text-decoration: underline;">https://apple.com</a>'
    );
    expect(lines[lines.length - 1]).toContain(
      '<a href="https://google.com" target="_blank" rel="noopener noreferrer" style="color: blue; text-decoration: underline;">https://google.com</a>'
    );
    expect(lines[lines.length - 1]).toContain(
      '<a href="https://something.logs/_dashboard/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:now-1d,to:now))&amp;_a=(columns:!(_source),filters:!((&#x27;$state&#x27;:(store:appState))))" target="_blank" rel="noopener noreferrer" style="color: blue; text-decoration: underline;">https://something.logs/_dashboard/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:now-1d,to:now))&amp;_a=(columns:!(_source),filters:!((&#x27;$state&#x27;:(store:appState))))</a>'
    );
  });
});
