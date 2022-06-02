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
import { ChakraProvider, Table, Tbody } from '@chakra-ui/react';
import { QueryClient, QueryClientProvider } from 'react-query';
import { MemoryRouter } from 'react-router-dom';
// eslint-disable-next-line import/no-extraneous-dependencies
import moment from 'moment-timezone';

import { ContainerRefProvider } from '../context/containerRef';
import { TimezoneProvider } from '../context/timezone';
import { AutoRefreshProvider } from '../context/autorefresh';

global.moment = moment;

export const Wrapper = ({ children }) => {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        cacheTime: Infinity,
        staleTime: Infinity,
      },
    },
  });
  return (
    <ChakraProvider>
      <QueryClientProvider client={queryClient}>
        <ContainerRefProvider>
          <TimezoneProvider>
            <AutoRefreshProvider>
              <MemoryRouter>
                {children}
              </MemoryRouter>
            </AutoRefreshProvider>
          </TimezoneProvider>
        </ContainerRefProvider>
      </QueryClientProvider>
    </ChakraProvider>
  );
};

export const ChakraWrapper = ({ children }) => (
  <ChakraProvider>
    {children}
  </ChakraProvider>
);

export const TableWrapper = ({ children }) => (
  <Wrapper>
    <Table>
      <Tbody>
        {children}
      </Tbody>
    </Table>
  </Wrapper>
);

export const RouterWrapper = ({ children }) => (
  <MemoryRouter>
    {children}
  </MemoryRouter>
);
