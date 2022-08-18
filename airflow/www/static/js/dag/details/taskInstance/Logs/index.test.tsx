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

/* global jest, describe, test, expect, beforeEach, window */

import React from 'react';
import { render, fireEvent } from '@testing-library/react';
import type { UseQueryResult } from 'react-query';

import * as utils from 'src/utils';
import * as useTaskLogModule from 'src/api/useTaskLog';

import Logs from './index';

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

let useTaskLogMock: jest.SpyInstance;

describe('Test Logs Component.', () => {
  const returnValue = {
    data: mockTaskLog,
    isSuccess: true,
  } as UseQueryResult<string, unknown>;

  beforeEach(() => {
    useTaskLogMock = jest.spyOn(useTaskLogModule, 'default').mockImplementation(() => returnValue);
    window.HTMLElement.prototype.scrollIntoView = jest.fn();
  });

  test('Test Logs Content', () => {
    const tryNumber = 2;
    const { getByText } = render(
      <Logs
        dagId="dummyDagId"
        dagRunId="dummyDagRunId"
        taskId="dummyTaskId"
        executionDate="2020:01:01T01:00+00:00"
        tryNumber={tryNumber}
      />,
    );
    expect(getByText('[2022-06-04, 00:00:01 UTC] {taskinstance.py:1329} INFO -', { exact: false })).toBeDefined();
    expect(getByText(
      '[2022-06-04, 00:00:01 UTC] {standard_task_runner.py:81} INFO - Job 1626: Subtask section_1.get_entry_group',
      { exact: false },
    )).toBeDefined();
    expect(getByText('AIRFLOW_CTX_DAG_ID=test_ui_grid', { exact: false })).toBeDefined();

    expect(useTaskLogMock).toHaveBeenLastCalledWith({
      dagId: 'dummyDagId',
      dagRunId: 'dummyDagRunId',
      fullContent: false,
      taskId: 'dummyTaskId',
      taskTryNumber: 1,
    });
  });

  test.each([
    { defaultWrap: 'True', shouldBeChecked: true },
    { defaultWrap: 'False', shouldBeChecked: false },
    { defaultWrap: '', shouldBeChecked: false },
  ])('Test wrap checkbox initial value $defaultWrap', ({ defaultWrap, shouldBeChecked }) => {
    jest.spyOn(utils, 'getMetaValue').mockImplementation(
      (meta) => {
        if (meta === 'default_wrap') return defaultWrap;
        return '';
      },
    );

    const tryNumber = 2;
    const { getByTestId } = render(
      <Logs
        dagId="dummyDagId"
        dagRunId="dummyDagRunId"
        taskId="dummyTaskId"
        executionDate="2020:01:01T01:00+00:00"
        mapIndex={1}
        tryNumber={tryNumber}
      />,
    );

    const wrapCheckbox = getByTestId('wrap-checkbox');
    if (shouldBeChecked) {
      expect(wrapCheckbox).toHaveAttribute('data-checked');
    } else {
      expect(wrapCheckbox.getAttribute('data-checked')).toBeNull();
    }
  });

  test('Test Logs Content Mapped Task', () => {
    const tryNumber = 2;
    const { getByText } = render(
      <Logs
        dagId="dummyDagId"
        dagRunId="dummyDagRunId"
        taskId="dummyTaskId"
        executionDate="2020:01:01T01:00+00:00"
        mapIndex={1}
        tryNumber={tryNumber}
      />,
    );
    expect(getByText('[2022-06-04, 00:00:01 UTC] {taskinstance.py:1329} INFO -', { exact: false })).toBeDefined();
    expect(getByText(
      '[2022-06-04, 00:00:01 UTC] {standard_task_runner.py:81} INFO - Job 1626: Subtask section_1.get_entry_group',
      { exact: false },
    )).toBeDefined();
    expect(getByText('AIRFLOW_CTX_DAG_ID=test_ui_grid', { exact: false })).toBeDefined();

    expect(useTaskLogMock).toHaveBeenLastCalledWith({
      dagId: 'dummyDagId',
      dagRunId: 'dummyDagRunId',
      fullContent: false,
      mapIndex: 1,
      taskId: 'dummyTaskId',
      taskTryNumber: 1,
    });
  });

  test('Test Logs Attempt Select Button', () => {
    const tryNumber = 2;
    const { getByText, getByTestId } = render(
      <Logs
        dagId="dummyDagId"
        dagRunId="dummyDagRunId"
        taskId="dummyTaskId"
        executionDate="2020:01:01T01:00+00:00"
        tryNumber={tryNumber}
      />,
    );
    // Internal Log Attempt buttons.
    expect(getByText('1')).toBeDefined();
    expect(getByText('2')).toBeDefined();

    expect(getByText('Download')).toBeDefined();

    expect(useTaskLogMock).toHaveBeenLastCalledWith({
      dagId: 'dummyDagId',
      dagRunId: 'dummyDagRunId',
      fullContent: false,
      taskId: 'dummyTaskId',
      taskTryNumber: 1,
    });
    const attemptButton2 = getByTestId('log-attempt-select-button-2');

    fireEvent.click(attemptButton2);

    expect(useTaskLogMock).toHaveBeenLastCalledWith({
      dagId: 'dummyDagId',
      dagRunId: 'dummyDagRunId',
      fullContent: false,
      taskId: 'dummyTaskId',
      taskTryNumber: 2,
    });
  });

  test('Test Logs Full Content', () => {
    const tryNumber = 2;
    const { getByTestId } = render(
      <Logs
        dagId="dummyDagId"
        dagRunId="dummyDagRunId"
        taskId="dummyTaskId"
        executionDate="2020:01:01T01:00+00:00"
        tryNumber={tryNumber}
      />,
    );
    expect(useTaskLogMock).toHaveBeenLastCalledWith({
      dagId: 'dummyDagId',
      dagRunId: 'dummyDagRunId',
      fullContent: false,
      taskId: 'dummyTaskId',
      taskTryNumber: 1,
    });
    const fullContentCheckbox = getByTestId('full-content-checkbox');

    fireEvent.click(fullContentCheckbox);

    expect(useTaskLogMock).toHaveBeenLastCalledWith({
      dagId: 'dummyDagId',
      dagRunId: 'dummyDagRunId',
      fullContent: true,
      taskId: 'dummyTaskId',
      taskTryNumber: 1,
    });
  });
});
