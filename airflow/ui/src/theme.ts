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
          background: "blue.minimal",
        },
        "th, td": {
          borderBottomWidth: "0px",
          borderColor: "blue.subtle",
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
  colors: {
    blue: {
      950: "#0c142e",
    },
  },
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
      blue: {
        /* eslint-disable perfectionist/sort-objects */
        contrast: { _dark: "blue.200", _light: "blue.600" },
        focusRing: "blue.500",
        fg: { _dark: "blue.600", _light: "blue.400" },
        emphasized: { _dark: "blue.700", _light: "blue.300" },
        solid: { _dark: "blue.800", _light: "blue.200" },
        muted: { _dark: "blue.900", _light: "blue.100" },
        subtle: { _dark: "blue.950", _light: "blue.50" },
        minimal: { _dark: "gray.900", _light: "blue.50" },
        /* eslint-enable perfectionist/sort-objects */
      },
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
