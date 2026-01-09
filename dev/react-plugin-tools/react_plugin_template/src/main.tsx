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
import { FC } from "react";

import { ColorModeProvider } from "src/context/colorMode";
import { HomePage } from "src/pages/HomePage";

import { localSystem } from "./theme";

export interface PluginComponentProps {
  // Add any props your plugin component needs
}

/**
 * Main plugin component
 */
const PluginComponent: FC<PluginComponentProps> = (props) => {

  // Use the globalChakraUISystem provided by the Airflow Core UI,
  // so the plugin has a consistent theming with the host Airflow UI,
  // fallback to localSystem for local development.
  const system = (globalThis.ChakraUISystem)  ?? localSystem;

  return (
    <ChakraProvider value={system}>
      <ColorModeProvider>
          <HomePage />
      </ColorModeProvider>
    </ChakraProvider>
  );
};

export default PluginComponent;
