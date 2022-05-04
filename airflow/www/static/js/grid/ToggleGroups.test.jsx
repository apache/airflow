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
import { Flex, Table, Tbody } from '@chakra-ui/react';
import { render, fireEvent, waitFor } from '@testing-library/react';

import renderTaskRows from './renderTaskRows';
import ToggleGroups from './ToggleGroups';
import { Wrapper } from './utils/testUtils';

const mockGridData = {
  groups: {
    id: null,
    label: null,
    children: [
      {
        extraLinks: [],
        id: 'group_1',
        label: 'group_1',
        instances: [
          {
            dagId: 'dagId',
            duration: 0,
            endDate: '2021-10-26T15:42:03.391939+00:00',
            executionDate: '2021-10-25T15:41:09.726436+00:00',
            operator: 'DummyOperator',
            runId: 'run1',
            startDate: '2021-10-26T15:42:03.391917+00:00',
            state: 'success',
            taskId: 'group_1',
            tryNumber: 1,
          },
        ],
        children: [
          {
            id: 'group_1.task_1',
            label: 'task_1',
            extraLinks: [],
            instances: [
              {
                dagId: 'dagId',
                duration: 0,
                endDate: '2021-10-26T15:42:03.391939+00:00',
                executionDate: '2021-10-25T15:41:09.726436+00:00',
                operator: 'DummyOperator',
                runId: 'run1',
                startDate: '2021-10-26T15:42:03.391917+00:00',
                state: 'success',
                taskId: 'group_1.task_1',
                tryNumber: 1,
              },
            ],
            children: [
              {
                id: 'group_1.task_1.sub_task_1',
                label: 'sub_task_1',
                extraLinks: [],
                instances: [
                  {
                    dagId: 'dagId',
                    duration: 0,
                    endDate: '2021-10-26T15:42:03.391939+00:00',
                    executionDate: '2021-10-25T15:41:09.726436+00:00',
                    operator: 'DummyOperator',
                    runId: 'run1',
                    startDate: '2021-10-26T15:42:03.391917+00:00',
                    state: 'success',
                    taskId: 'group_1.task_1.sub_task_1',
                    tryNumber: 1,
                  },
                ],
              },
            ],
          },
        ],
      },
    ],
    instances: [],
  },
  dagRuns: [
    {
      dagId: 'dagId',
      runId: 'run1',
      dataIntervalStart: new Date(),
      dataIntervalEnd: new Date(),
      startDate: '2021-11-08T21:14:19.704433+00:00',
      endDate: '2021-11-08T21:17:13.206426+00:00',
      state: 'failed',
      runType: 'scheduled',
      executionDate: '2021-11-08T21:14:19.704433+00:00',
    },
  ],
};

describe('Test ToggleGroups', () => {
  test('Button text changes on click', () => {
    const { getByText } = render(
      <ToggleGroups groups={mockGridData.groups} />,
      { wrapper: Wrapper },
    );

    const toggleButton = getByText('Expand all');

    expect(toggleButton).toBeInTheDocument();

    fireEvent.click(toggleButton);

    expect(getByText('Collapse all')).toBeInTheDocument();
  });

  test('Toggle button opens and closes nested groups', async () => {
    global.gridData = mockGridData;
    const dagRunIds = mockGridData.dagRuns.map((dr) => dr.runId);
    const task = mockGridData.groups;

    const { getByText, queryAllByTestId } = render(
      <Flex>
        <ToggleGroups groups={task} />
        <Table>
          <Tbody>
            {renderTaskRows({ task, dagRunIds })}
          </Tbody>
        </Table>
      </Flex>,
      { wrapper: Wrapper },
    );

    const toggleButton = getByText('Collapse all');

    const groupName = getByText('group_1');

    expect(queryAllByTestId('task-instance')).toHaveLength(3);
    expect(groupName).toBeInTheDocument();

    expect(queryAllByTestId('open-group')).toHaveLength(2);
    expect(queryAllByTestId('closed-group')).toHaveLength(0);

    fireEvent.click(toggleButton);

    await waitFor(() => expect(queryAllByTestId('task-instance')).toHaveLength(1));
    expect(queryAllByTestId('open-group')).toHaveLength(0);
    // Since the groups are nested, only the parent row is rendered
    expect(queryAllByTestId('closed-group')).toHaveLength(1);

    fireEvent.click(toggleButton);

    await waitFor(() => expect(queryAllByTestId('task-instance')).toHaveLength(3));
    expect(queryAllByTestId('open-group')).toHaveLength(2);
    expect(queryAllByTestId('closed-group')).toHaveLength(0);
  });
});
