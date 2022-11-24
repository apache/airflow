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

import { TableWrapper } from 'src/utils/testUtils';
import type { Task } from 'src/types';

import renderTaskRows from './renderTaskRows';

describe('Test renderTaskRows', () => {
  test('Renders name and task instance', () => {
    const task: Task = {
      id: null,
      label: null,
      children: [
        {
          extraLinks: [],
          id: 'group_1',
          label: 'group_1',
          instances: [
            {
              endDate: '2021-10-26T15:42:03.391939+00:00',
              runId: 'run1',
              startDate: '2021-10-26T15:42:03.391917+00:00',
              state: 'success',
              taskId: 'group_1',
              note: '',
            },
          ],
          children: [
            {
              id: 'group_1.task_1',
              label: 'group_1.task_1',
              extraLinks: [],
              instances: [
                {
                  endDate: '2021-10-26T15:42:03.391939+00:00',
                  runId: 'run1',
                  startDate: '2021-10-26T15:42:03.391917+00:00',
                  state: 'success',
                  taskId: 'group_1.task_1',
                  note: '',
                },
              ],
            },
          ],
        },
      ],
      instances: [],
    };

    const { queryByTestId, getByText } = render(
      <>{renderTaskRows({ task, dagRunIds: ['run1'] })}</>,
      { wrapper: TableWrapper },
    );

    expect(getByText('group_1')).toBeInTheDocument();
    expect(queryByTestId('task-instance')).toBeDefined();
    expect(queryByTestId('blank-task')).toBeNull();
  });

  test('Still renders names if there are no instances', () => {
    const task = {
      id: null,
      label: null,
      children: [
        {
          extraLinks: [],
          id: 'group_1',
          label: 'group_1',
          instances: [],
        },
      ],
      instances: [],
    };

    const { queryByTestId, getByText } = render(
      <>{renderTaskRows({ task, dagRunIds: [] })}</>,
      { wrapper: TableWrapper },
    );

    expect(getByText('group_1')).toBeInTheDocument();
    expect(queryByTestId('task-instance')).toBeNull();
  });

  test('Still renders correctly if task instance is null', () => {
    const task: Task = {
      id: null,
      label: null,
      children: [
        {
          extraLinks: [],
          id: 'group_1',
          label: 'group_1',
          instances: [],
          children: [
            {
              id: 'group_1.task_1',
              label: 'group_1.task_1',
              extraLinks: [],
              instances: [],
            },
          ],
        },
      ],
      instances: [],
    };

    const { queryByTestId, getByText } = render(
      <>{renderTaskRows({ task, dagRunIds: ['run1'] })}</>,
      { wrapper: TableWrapper },
    );

    expect(getByText('group_1')).toBeInTheDocument();
    expect(queryByTestId('task-instance')).toBeNull();
    expect(queryByTestId('blank-task')).toBeInTheDocument();
  });
});
