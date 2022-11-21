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

import { isEmpty } from 'lodash';
import type { DagRun } from 'src/types';
import {
  getDagRunLabel, getTask, getTaskSummary,
} from '.';

const sampleTasks = {
  id: null,
  instances: [],
  label: null,
  children: [
    {
      id: 'task_1',
      label: 'task_1',
      instances: [],
      operator: 'BashOperator',
    },
    {
      id: 'group_task',
      label: 'group_task',
      instances: [],
      children: [
        {
          id: 'group_task.task_2',
          label: 'task_2',
          instances: [],
          children: [
            {
              id: 'group_task.task_2.nested',
              label: 'nested',
              instances: [],
              operator: 'BashOperator',
            },
          ],
        },
      ],
    },
    {
      id: 'task_3',
      label: 'task_3',
      instances: [],
      children: [{
        id: 'task_3.child',
        label: 'child',
        instances: [],
        operator: 'PythonOperator',
      }],
    },
  ],
};

describe('Test getTask()', () => {
  test('Can get a nested task_id', async () => {
    const task = getTask({ taskId: 'task_3.child', task: sampleTasks });
    expect(task?.label).toBe('child');
  });

  test('Can get a group', async () => {
    const task = getTask({ taskId: 'group_task', task: sampleTasks });
    expect(task?.label).toBe('group_task');
  });

  test('Can get a multi-nested task', async () => {
    const task = getTask({ taskId: 'group_task.task_2.nested', task: sampleTasks });
    expect(task?.label).toBe('nested');
  });
});

describe('Test getTaskSummary()', () => {
  test('Counts tasks, groups, and operators with deep nesting', async () => {
    const summary = getTaskSummary({ task: sampleTasks });

    expect(summary.groupCount).toBe(3);
    expect(summary.taskCount).toBe(3);
    expect(summary.operators.BashOperator).toBe(2);
    expect(summary.operators.PythonOperator).toBe(1);
  });

  test('Returns 0 groups and no operators', async () => {
    const noOperators = {
      id: null,
      instances: [],
      label: null,
      children: [
        {
          id: 'task_1',
          label: 'task_1',
          instances: [],
        },
      ],
    };
    const summary = getTaskSummary({ task: noOperators });

    expect(summary.groupCount).toBe(0);
    expect(summary.taskCount).toBe(1);
    expect(isEmpty(summary.operators)).toBeTruthy();
  });
});

describe('Test getDagRunLabel', () => {
  const dagRun = {
    dagId: 'dagId',
    runId: 'run1',
    dataIntervalStart: '2021-12-07T21:14:19.704433+00:00',
    dataIntervalEnd: '2021-12-08T21:14:19.704433+00:00',
    queuedAt: '2021-11-08T21:14:18.21521+00:00',
    startDate: '2021-11-08T21:14:19.704433+00:00',
    endDate: '2021-11-08T21:17:13.206426+00:00',
    state: 'failed',
    runType: 'scheduled',
    executionDate: '2021-12-09T21:14:19.704433+00:00',
    lastSchedulingDecision: '2021-11-08T21:14:19.704433+00:00',
    externalTrigger: false,
    conf: null,
    confIsJson: false,
    notes: 'someRandomValue',
  } as DagRun;

  test('Defaults to dataIntervalEnd', async () => {
    const runLabel = getDagRunLabel({ dagRun });
    expect(runLabel).toBe(dagRun.dataIntervalEnd);
  });

  test('Passing an order overrides default', async () => {
    const runLabel = getDagRunLabel({ dagRun, ordering: ['executionDate'] });
    expect(runLabel).toBe(dagRun.executionDate);
  });
});
