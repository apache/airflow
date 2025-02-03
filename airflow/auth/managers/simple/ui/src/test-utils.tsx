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

import React, { PropsWithChildren } from "react";
import { ChakraProvider, defaultSystem } from "@chakra-ui/react";
import { MemoryRouter, MemoryRouterProps } from "react-router-dom";
import {QueryClient, QueryClientProvider} from "@tanstack/react-query";

interface WrapperProps extends PropsWithChildren {
  initialEntries?: MemoryRouterProps["initialEntries"];
}

export const Wrapper = ({ initialEntries, children }: WrapperProps) => {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        staleTime: Infinity,
      },
    },
  });
  return (
    <ChakraProvider value={defaultSystem}>
      <QueryClientProvider client={queryClient}>
        <MemoryRouter initialEntries={initialEntries}>
          {children}
        </MemoryRouter>
      </QueryClientProvider>
    </ChakraProvider>
  );
};
