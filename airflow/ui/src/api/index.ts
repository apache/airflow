/* eslint-disable no-console */
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

import axios, { AxiosResponse } from 'axios';
import { useQuery, setLogger } from 'react-query';
import humps from 'humps';

import type { Version } from 'interfaces';
import type { DagsResponse } from 'interfaces/api';

axios.defaults.baseURL = `${process.env.WEBSERVER_URL}/api/v1`;
axios.interceptors.response.use(
  (res) => (res.data ? humps.camelizeKeys(res.data) as unknown as AxiosResponse : res),
);

// turn off logging, retry and refetch on tests
const isTest = process.env.NODE_ENV === 'test';

setLogger({
  log: isTest ? () => {} : console.log,
  warn: isTest ? () => {} : console.warn,
  error: isTest ? () => {} : console.warn,
});

const refetchInterval = isTest ? false : 1000;

export function useDags() {
  return useQuery<DagsResponse, Error>(
    'dags',
    (): Promise<DagsResponse> => axios.get('/dags'),
    {
      refetchInterval,
      retry: !isTest,
    },
  );
}

export function useVersion() {
  return useQuery<Version, Error>(
    'version',
    (): Promise<Version> => axios.get('/version'),
  );
}
