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

// Chakra has bad types for this util, so is registered as unbound
// eslint-disable-next-line @typescript-eslint/unbound-method
const { defineMultiStyleConfig, definePartsStyle } =
  createMultiStyleConfigHelpers(tableAnatomy.keys);

const baseStyle = definePartsStyle((props) => {
  const { colorMode, colorScheme } = props;

  return {
    tbody: {
      tr: {
        "&:nth-of-type(even)": {
          "th, td": {
            borderBottomWidth: "0px",
          },
        },
        "&:nth-of-type(odd)": {
          td: {
            background:
              colorMode === "light" ? `${colorScheme}.50` : `gray.900`,
          },
          "th, td": {
            borderBottomWidth: "0px",
            borderColor:
              colorMode === "light" ? `${colorScheme}.50` : `gray.900`,
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
  };
});

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
    useSystemColorMode: true,
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
