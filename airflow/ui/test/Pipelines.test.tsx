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

import React from 'react';
import '@testing-library/jest-dom';
import { render, waitFor } from '@testing-library/react';
import nock from 'nock';

import Pipelines from 'views/Pipelines';
import {
  defaultHeaders, QueryWrapper, RouterWrapper, url,
} from './utils';

const sampleDag = {
  dagId: 'dagId1',
  description: 'string',
  fileToken: 'string',
  fileloc: 'string',
  isPaused: false,
  isSubdag: true,
  rootDagId: 'string',
  owners: [
    'string',
  ],
  tags: [
    {
      name: 'string',
    },
  ],
};

nock(url)
  .defaultReplyHeaders(defaultHeaders)
  .persist()
  .get('/version')
  .reply(200, { version: '', gitVersion: '' });

describe('Test Pipelines Table', () => {
  test('Show a loading indicator and have a DAG count of 0 before data loads', async () => {
    nock(url)
      .defaultReplyHeaders(defaultHeaders)
      .get('/dags')
      .reply(200, {
        dags: [sampleDag],
        totalEntries: 1,
      });

    const { getByText } = render(
      <QueryWrapper><Pipelines /></QueryWrapper>,
      {
        wrapper: RouterWrapper,
      },
    );
    expect(getByText('Loading…')).toBeInTheDocument();
    await waitFor(() => expect(getByText(sampleDag.dagId)).toBeInTheDocument());
  });

  test('Show Empty State text if there are no dags', () => {
    nock(url)
      .defaultReplyHeaders(defaultHeaders)
      .get('/dags')
      .reply(404, undefined);

    const { getByText } = render(
      <QueryWrapper><Pipelines /></QueryWrapper>,
      {
        wrapper: RouterWrapper,
      },
    );
    waitFor(() => expect(getByText('No DAGs found.')).toBeInTheDocument());
  });
});
