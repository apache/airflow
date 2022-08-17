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

/* global document */

import React from 'react';
import { createRoot } from 'react-dom/client';
import createCache from '@emotion/cache';
import { useSearchParams } from 'react-router-dom';
import { Center } from '@chakra-ui/react';

import App from 'src/App';

import DatasetsList from './List';
import DatasetDetails from './Details';

// create shadowRoot
const root = document.querySelector('#root');
const shadowRoot = root?.attachShadow({ mode: 'open' });
const cache = createCache({
  container: shadowRoot,
  key: 'c',
});
const mainElement = document.getElementById('react-container');

const DATASET_URI = 'dataset_uri';

const Datasets = () => {
  const [searchParams, setSearchParams] = useSearchParams();

  const onBack = () => {
    searchParams.delete(DATASET_URI);
    setSearchParams(searchParams);
  };

  const onSelect = (datasetUri: string) => {
    searchParams.set(DATASET_URI, datasetUri);
    setSearchParams(searchParams);
  };

  const datasetUri = decodeURIComponent(searchParams.get(DATASET_URI) || '');
  return (datasetUri
    ? <DatasetDetails datasetUri={datasetUri} onBack={onBack} />
    : <DatasetsList onSelect={onSelect} />
  );
};

if (mainElement) {
  shadowRoot?.appendChild(mainElement);
  const reactRoot = createRoot(mainElement);
  reactRoot.render(
    <App cache={cache}>
      <Center>
        <Datasets />
      </Center>
    </App>,
  );
}
