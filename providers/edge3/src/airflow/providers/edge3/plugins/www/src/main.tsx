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
import { ChakraProvider } from "@chakra-ui/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { OpenAPI } from "openapi/requests/core/OpenAPI";
import { FC } from "react";

import { ColorModeProvider } from "src/context/colorMode";
import { EdgeLayout } from "src/layouts/EdgeLayout";

import { system } from "./theme";

export type PluginComponentProps = object;

/**
 * Main plugin component
 */
const PluginComponent: FC<PluginComponentProps> = () => {
  // Set the base URL for OpenAPI client from the HTML base tag
  const baseHref = document.querySelector("head > base")?.getAttribute("href") ?? "";
  const baseUrl = new URL(baseHref, globalThis.location.origin);
  OpenAPI.BASE = baseUrl.pathname.replace(/\/$/, ""); // Remove trailing slash

  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        staleTime: Infinity,
      },
    },
  });

  return (
    <ChakraProvider value={system}>
      <QueryClientProvider client={queryClient}>
        <ColorModeProvider>
          <EdgeLayout />
        </ColorModeProvider>
      </QueryClientProvider>
    </ChakraProvider>
  );
};

export default PluginComponent;
