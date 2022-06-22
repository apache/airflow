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

import InstanceTooltip from './InstanceTooltip';
import { Wrapper } from '../utils/testUtils';
import type { TaskState } from '../types';

const instance = {
  startDate: new Date().toISOString(),
  endDate: new Date().toISOString(),
  state: 'success' as TaskState,
  runId: 'run',
  taskId: 'task',
};

describe('Test Task InstanceTooltip', () => {
  test('Displays a normal task', () => {
    const { getByText } = render(
      <InstanceTooltip
        group={{ id: 'task', label: 'task', instances: [] }}
        instance={instance}
      />,
      { wrapper: Wrapper },
    );

    expect(getByText('Status: success')).toBeDefined();
  });

  test('Displays a mapped task with overall status', () => {
    const { getByText } = render(
      <InstanceTooltip
        group={{
          id: 'task', label: 'task', instances: [], isMapped: true,
        }}
        instance={{ ...instance, mappedStates: { success: 2 } }}
      />,
      { wrapper: Wrapper },
    );

    expect(getByText('Overall Status: success')).toBeDefined();
    expect(getByText('2 mapped tasks')).toBeDefined();
    expect(getByText('success: 2')).toBeDefined();
  });

  test('Displays a task group with overall status', () => {
    const { getByText, queryByText } = render(
      <InstanceTooltip
        group={{
          id: 'task',
          label: 'task',
          instances: [],
          children: [
            {
              id: 'child_task',
              label: 'child_task',
              instances: [
                {
                  taskId: 'child_task',
                  runId: 'run',
                  state: 'success',
                  startDate: '',
                  endDate: '',
                },
              ],
            },
          ],
        }}
        instance={instance}
      />,
      { wrapper: Wrapper },
    );

    expect(getByText('Overall Status: success')).toBeDefined();
    expect(queryByText('mapped task')).toBeNull();
    expect(getByText('success: 1')).toBeDefined();
  });
});
