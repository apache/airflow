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

/*
  Base setup for anywhere we add react to the UI
*/

import React, { PropsWithChildren } from 'react';
import { BrowserRouter } from 'react-router-dom';
import { ChakraProvider } from '@chakra-ui/react';
import { CacheProvider } from '@emotion/react';
import type { EmotionCache } from '@emotion/cache';
import { QueryClient, QueryClientProvider } from 'react-query';

import theme from './theme';
import { ContainerRefProvider, useContainerRef } from './context/containerRef';
import { TimezoneProvider } from './context/timezone';
import { AutoRefreshProvider } from './context/autorefresh';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      notifyOnChangeProps: 'tracked',
      refetchOnWindowFocus: false,
      retry: 1,
      retryDelay: 500,
      refetchOnMount: true, // Refetches stale queries, not "always"
      staleTime: 5 * 60 * 1000, // 5 minutes
      initialDataUpdatedAt: new Date().setMinutes(-6), // make sure initial data is already expired
    },
    mutations: {
      retry: 1,
      retryDelay: 500,
    },
  },
});

interface AppProps extends PropsWithChildren {
  cache: EmotionCache;
}

// Chakra needs to access the containerRef provider so our tooltips pick up the correct styles
const ChakraApp = ({ children }: PropsWithChildren) => {
  const containerRef = useContainerRef();
  return (
    <ChakraProvider theme={theme} toastOptions={{ portalProps: { containerRef } }}>
      <QueryClientProvider client={queryClient}>
        <TimezoneProvider>
          <AutoRefreshProvider>
            <BrowserRouter>
              {children}
            </BrowserRouter>
          </AutoRefreshProvider>
        </TimezoneProvider>
      </QueryClientProvider>
    </ChakraProvider>
  );
};

function App({ children, cache }: AppProps) {
  return (
    <React.StrictMode>
      <CacheProvider value={cache}>
        <ContainerRefProvider>
          <ChakraApp>
            {children}
          </ChakraApp>
        </ContainerRefProvider>
      </CacheProvider>
    </React.StrictMode>
  );
}

export default App;
