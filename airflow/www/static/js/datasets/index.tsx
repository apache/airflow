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

import React, { useRef } from 'react';
import { createRoot } from 'react-dom/client';
import createCache from '@emotion/cache';
import { useSearchParams } from 'react-router-dom';
import { Flex, Box, useDimensions } from '@chakra-ui/react';

import App from 'src/App';
import useContentHeight from 'src/utils/useContentHeight';

import DatasetsList from './List';
import DatasetDetails from './Details';
import Graph from './Graph';

// create shadowRoot
const root = document.querySelector('#root');
const shadowRoot = root?.attachShadow({ mode: 'open' });
const cache = createCache({
  container: shadowRoot,
  key: 'c',
});
const mainElement = document.getElementById('react-container');

const DATASET_URI = 'uri';

const Datasets = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const contentRef = useRef<HTMLDivElement>(null);
  const graphRef = useRef<HTMLDivElement>(null);
  const dimensions = useDimensions(graphRef, true);

  const onBack = () => {
    searchParams.delete(DATASET_URI);
    setSearchParams(searchParams);
  };

  const onSelect = (datasetUri: string) => {
    searchParams.set(DATASET_URI, encodeURIComponent(datasetUri));
    setSearchParams(searchParams);
  };

  const datasetUri = decodeURIComponent(searchParams.get(DATASET_URI) || '');

  useContentHeight(contentRef);

  return (
    <Flex alignItems="flex-start" justifyContent="space-between" ref={contentRef}>
      <Box minWidth="450px" height="100%" overflowY="scroll">
        {datasetUri
          ? <DatasetDetails uri={datasetUri} onBack={onBack} />
          : <DatasetsList onSelect={onSelect} />}
      </Box>
      <Box flex={1} ref={graphRef} height="100%" borderColor="gray.200" borderWidth={1}>
        <Graph
          selectedUri={datasetUri}
          onSelect={onSelect}
          height={dimensions?.contentBox.height || 0}
          width={dimensions?.contentBox.width || 0}
        />
      </Box>
    </Flex>
  );
};

if (mainElement) {
  shadowRoot?.appendChild(mainElement);
  const reactRoot = createRoot(mainElement);
  reactRoot.render(
    <App cache={cache}>
      <Datasets />
    </App>,
  );
}
