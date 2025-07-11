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

import { createSystem, defaultConfig, defineConfig } from "@chakra-ui/react";

const customConfig = defineConfig({
  theme: {
    tokens: {
      colors: {
        // Add your custom colors here
        primary: {
          "50": { value: "#E6F3FF" },
          "100": { value: "#BAE0FF" },
          "200": { value: "#7CC7FF" },
          "300": { value: "#47B2FF" },
          "400": { value: "#2196F3" },
          "500": { value: "#1976D2" },
          "600": { value: "#1565C0" },
          "700": { value: "#0D47A1" },
          "800": { value: "#0A3D91" },
          "900": { value: "#063781" },
        },
      },
    },
  },
});

export const system = createSystem(defaultConfig, customConfig);
