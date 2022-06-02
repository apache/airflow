/* eslint-disable class-methods-use-this */
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

/* global describe, test, expect, beforeEach, beforeAll, jest, window */

import React from 'react';
import { render, fireEvent, waitFor } from '@testing-library/react';

import Grid from './Grid';
import { Wrapper } from './utils/testUtils';
import * as useGridDataModule from './api/useGridData';

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

const EXPAND = 'Expand all task groups';
const COLLAPSE = 'Collapse all task groups';

describe('Test ToggleGroups', () => {
  beforeAll(() => {
    class ResizeObserver {
      observe() {}

      unobserve() {}

      disconnect() {}
    }

    window.ResizeObserver = ResizeObserver;
  });

  beforeEach(() => {
    jest.spyOn(useGridDataModule, 'default').mockImplementation(() => ({
      data: mockGridData,
    }));
  });

  test('Group defaults to closed', () => {
    const { getByTestId, getByText, getAllByTestId } = render(
      <Grid />,
      { wrapper: Wrapper },
    );

    const groupName = getByText('group_1');

    expect(getAllByTestId('task-instance')).toHaveLength(1);
    expect(groupName).toBeInTheDocument();
    expect(getByTestId('closed-group')).toBeInTheDocument();
  });

  test('Buttons are disabled if all groups are expanded or collapsed', () => {
    const { getByTitle } = render(
      <Grid />,
      { wrapper: Wrapper },
    );

    const expandButton = getByTitle(EXPAND);
    const collapseButton = getByTitle(COLLAPSE);

    expect(expandButton).toBeEnabled();
    expect(collapseButton).toBeDisabled();

    fireEvent.click(expandButton);

    expect(collapseButton).toBeEnabled();
    expect(expandButton).toBeDisabled();
  });

  test('Expand/collapse buttons toggle nested groups', async () => {
    const { getByText, queryAllByTestId, getByTitle } = render(
      <Grid />,
      { wrapper: Wrapper },
    );

    const expandButton = getByTitle(EXPAND);
    const collapseButton = getByTitle(COLLAPSE);

    const groupName = getByText('group_1');

    expect(queryAllByTestId('task-instance')).toHaveLength(3);
    expect(groupName).toBeInTheDocument();

    expect(queryAllByTestId('open-group')).toHaveLength(2);
    expect(queryAllByTestId('closed-group')).toHaveLength(0);

    fireEvent.click(collapseButton);

    await waitFor(() => expect(queryAllByTestId('task-instance')).toHaveLength(1));
    expect(queryAllByTestId('open-group')).toHaveLength(0);
    // Since the groups are nested, only the parent row is rendered
    expect(queryAllByTestId('closed-group')).toHaveLength(1);

    fireEvent.click(expandButton);

    await waitFor(() => expect(queryAllByTestId('task-instance')).toHaveLength(3));
    expect(queryAllByTestId('open-group')).toHaveLength(2);
    expect(queryAllByTestId('closed-group')).toHaveLength(0);
  });

  test('Hovered effect on task state', async () => {
    const { rerender, queryAllByTestId } = render(
      <Grid />,
      { wrapper: Wrapper },
    );

    const taskElements = queryAllByTestId('task-instance');
    expect(taskElements).toHaveLength(3);

    taskElements.forEach((taskElement) => {
      expect(taskElement).toHaveStyle('opacity: 1');
    });

    rerender(
      <Grid hoveredTaskState="success" />,
      { wrapper: Wrapper },
    );

    taskElements.forEach((taskElement) => {
      expect(taskElement).toHaveStyle('opacity: 1');
    });

    rerender(
      <Grid hoveredTaskState="failed" />,
      { wrapper: Wrapper },
    );

    taskElements.forEach((taskElement) => {
      expect(taskElement).toHaveStyle('opacity: 0.3');
    });
  });
});
