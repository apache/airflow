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
import { tableAnatomy } from "@chakra-ui/anatomy";
import { createMultiStyleConfigHelpers, extendTheme } from "@chakra-ui/react";

const { defineMultiStyleConfig, definePartsStyle } =
  createMultiStyleConfigHelpers(tableAnatomy.keys);

const baseStyle = definePartsStyle(() => ({
  tbody: {
    tr: {
      "&:nth-of-type(even)": {
        "th, td": {
          borderBottomWidth: "0px",
        },
      },
      "&:nth-of-type(odd)": {
        td: {
          background: "subtle-bg",
        },
        "th, td": {
          borderBottomWidth: "0px",
          borderColor: "subtle-bg",
        },
      },
    },
  },
  thead: {
    tr: {
      th: {
        borderBottomWidth: 0,
      },
    },
  },
}));

export const tableTheme = defineMultiStyleConfig({ baseStyle });

const theme = extendTheme({
  components: {
    Table: tableTheme,
    Tooltip: {
      baseStyle: {
        fontSize: "md",
      },
    },
  },
  config: {
    initialColorMode: "system",
    useSystemColorMode: true,
  },
  semanticTokens: {
    colors: {
      "subtle-bg": { _dark: "gray.900", _light: "blue.50" },
      "subtle-text": { _dark: "blue.500", _light: "blue.600" },
    },
  },
  styles: {
    global: {
      "*, *::before, &::after": {
        borderColor: "gray.200",
      },
    },
  },
});

export default theme;
