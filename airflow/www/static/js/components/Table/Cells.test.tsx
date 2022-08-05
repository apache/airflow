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
import '@testing-library/jest-dom';
import { render } from '@testing-library/react';

import { ChakraWrapper } from 'src/utils/testUtils';
import * as utils from 'src/utils';
import { TaskInstanceLink } from './Cells';

const taskId = 'task_id';
const sourceDagId = 'source_dag_id';
const sourceRunId = 'source_run_id';
const originalDagId = 'og_dag_id';

describe('Test TaskInstanceLink', () => {
  test('Replaces __DAG_ID__ url param correctly', async () => {
    jest.spyOn(utils, 'getMetaValue').mockImplementation(
      (meta) => {
        if (meta === 'grid_url') return '/dags/__DAG_ID__/grid';
        return null;
      },
    );

    const { getByText } = render(
      <TaskInstanceLink
        cell={{
          value: taskId,
          row: {
            original: {
              sourceRunId,
              sourceDagId,
              sourceMapIndex: -1,
            },
          },
        }}
      />,
      { wrapper: ChakraWrapper },
    );

    const link = getByText(`${sourceDagId}.${taskId}`);
    expect(link).toBeInTheDocument();
    expect(link).toHaveAttribute('href', `/dags/${sourceDagId}/grid?dag_run_id=${sourceRunId}&task_id=${taskId}`);
  });

  test('Replaces existing dag id url param correctly', async () => {
    jest.spyOn(utils, 'getMetaValue').mockImplementation(
      (meta) => {
        if (meta === 'dag_id') return originalDagId;
        if (meta === 'grid_url') return `/dags/${originalDagId}/grid`;
        return null;
      },
    );

    const { getByText } = render(
      <TaskInstanceLink
        cell={{
          value: taskId,
          row: {
            original: {
              sourceRunId,
              sourceDagId,
              sourceMapIndex: -1,
            },
          },
        }}
      />,
      { wrapper: ChakraWrapper },
    );

    const link = getByText(`${sourceDagId}.${taskId}`);
    expect(link).toBeInTheDocument();
    expect(link).toHaveAttribute('href', `/dags/${sourceDagId}/grid?dag_run_id=${sourceRunId}&task_id=${taskId}`);
  });
});
