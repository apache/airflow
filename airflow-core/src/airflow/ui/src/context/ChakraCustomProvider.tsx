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
import { useMemo, type PropsWithChildren } from "react";

import type { Theme } from "openapi/requests/types.gen";
import { useConfig } from "src/queries/useConfig";
import { createTheme } from "src/theme";

export const ChakraCustomProvider = ({ children }: PropsWithChildren) => {
  const theme = useConfig("theme");

  const system = useMemo(() => {
    if (typeof theme === "undefined") {
      return undefined;
    }

    const syst = createTheme(theme as Theme);

    // Once the system is created, make it globally available to dynamically imported React plugins.
    Reflect.set(globalThis, "ChakraUISystem", syst);

    return syst;
  }, [theme]);

  return system && <ChakraProvider value={system}>{children}</ChakraProvider>;
};
