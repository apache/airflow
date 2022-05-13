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
import ReactDOM from 'react-dom';
import { BrowserRouter } from 'react-router-dom';
import { ChakraProvider, extendTheme } from '@chakra-ui/react';
import { CacheProvider } from '@emotion/react';
import createCache from '@emotion/cache';
import { QueryClient, QueryClientProvider } from 'react-query';

import Main from './Main';
import { ContainerRefProvider } from './context/containerRef';
import { TimezoneProvider } from './context/timezone';
import { AutoRefreshProvider } from './context/autorefresh';

// create shadowRoot
const root = document.querySelector('#root');
const shadowRoot = root.attachShadow({ mode: 'open' });
const myCache = createCache({
  container: shadowRoot,
  key: 'c',
});
const mainElement = document.getElementById('react-container');
shadowRoot.appendChild(mainElement);

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      retry: 1,
      retryDelay: 500,
      refetchOnMount: true, // Refetches stale queries, not "always"
    },
    mutations: {
      retry: 1,
      retryDelay: 500,
    },
  },
});

const theme = extendTheme({
  components: {
    Tooltip: {
      baseStyle: {
        fontSize: 'md',
      },
    },
  },
});

function App() {
  return (
    <React.StrictMode>
      <CacheProvider value={myCache}>
        <ChakraProvider theme={theme}>
          <ContainerRefProvider>
            <QueryClientProvider client={queryClient}>
              <TimezoneProvider>
                <AutoRefreshProvider>
                  <BrowserRouter>
                    <Main />
                  </BrowserRouter>
                </AutoRefreshProvider>
              </TimezoneProvider>
            </QueryClientProvider>
          </ContainerRefProvider>
        </ChakraProvider>
      </CacheProvider>
    </React.StrictMode>
  );
}

ReactDOM.render(<App />, mainElement);
