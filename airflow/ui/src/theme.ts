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

import { extendTheme } from "@chakra-ui/react";
import { tableAnatomy } from "@chakra-ui/anatomy";
import { createMultiStyleConfigHelpers } from "@chakra-ui/react";

const { definePartsStyle, defineMultiStyleConfig } =
  createMultiStyleConfigHelpers(tableAnatomy.keys);

const baseStyle = definePartsStyle((props) => {
  const { colorScheme: c, colorMode } = props;
  return {
    thead: {
      tr: {
        th: {
          borderBottomWidth: 0,
        },
      },
    },
    tbody: {
      tr: {
        "&:nth-of-type(odd)": {
          "th, td": {
            borderBottomWidth: "0px",
            borderColor: colorMode === "light" ? `${c}.50` : `gray.900`,
          },
          td: {
            background: colorMode === "light" ? `${c}.50` : `gray.900`,
          },
        },
        "&:nth-of-type(even)": {
          "th, td": {
            borderBottomWidth: "0px",
          },
        },
      },
    },
  };
});

export const tableTheme = defineMultiStyleConfig({ baseStyle });

const theme = extendTheme({
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
  components: {
    Tooltip: {
      baseStyle: {
        fontSize: "md",
      },
    },
    Table: tableTheme,
  },
});

export default theme;
