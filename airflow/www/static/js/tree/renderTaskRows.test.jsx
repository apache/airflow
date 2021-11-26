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

/* global describe, test, expect, jest */

import React from 'react';
import { render, fireEvent } from '@testing-library/react';
import { ChakraProvider, Table, Tbody } from '@chakra-ui/react';
import moment from 'moment';

import renderTaskRows from './renderTaskRows';

// Mock modal open function we take from dag.js
jest.mock('../dag', () => ({
  callModalDag: () => {},
  callModal: () => {},
}));
global.moment = moment;

const mockTreeData = {
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
            label: 'group_1.task_1',
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

describe('Test renderTaskRows', () => {
  test('Group defaults to closed but clicking on the name will open a group', () => {
    global.treeData = mockTreeData;
    const containerRef = {};

    const { getByTestId, getByText, getAllByTestId } = render(
      <React.StrictMode>
        <ChakraProvider>
          <Table>
            <Tbody>
              {renderTaskRows({ task: mockTreeData.groups, containerRef })}
            </Tbody>
          </Table>
        </ChakraProvider>
      </React.StrictMode>,
    );

    const groupName = getByText('group_1');

    expect(getAllByTestId('task-instance')).toHaveLength(2);
    expect(groupName).toBeInTheDocument();
    expect(getByTestId('closed-group')).toBeInTheDocument();

    fireEvent.click(groupName);

    expect(getByTestId('open-group')).toBeInTheDocument();
  });

  test('Still renders names if there are no instances', () => {
    global.treeData = {
      groups: {
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
      },
      dagRuns: [],
    };
    const containerRef = {};

    const { queryByTestId, getByText } = render(
      <React.StrictMode>
        <ChakraProvider>
          <Table>
            <Tbody>
              {renderTaskRows({ task: mockTreeData.groups, containerRef })}
            </Tbody>
          </Table>
        </ChakraProvider>
      </React.StrictMode>,
    );

    expect(getByText('group_1')).toBeInTheDocument();
    expect(queryByTestId('task-instance')).toBeNull();
  });
});
