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

import { extendTheme, createMultiStyleConfigHelpers } from "@chakra-ui/react";
import { tableAnatomy } from "@chakra-ui/anatomy";

const { definePartsStyle, defineMultiStyleConfig } =
  createMultiStyleConfigHelpers(tableAnatomy.keys);

const variantRounded = definePartsStyle((props) => {
  const { colorScheme: c } = props;

  return {
    tbody: {
      tr: {
        "&:nth-of-type(odd)": {
          "th, td": {
            borderBottomWidth: "1px",
            borderColor: `${c}.100`,
          },
          td: {
            background: "white",
          },
        },
        "&:nth-of-type(even)": {
          "th, td": {
            borderBottomWidth: "1px",
            borderColor: `${c}.100`,
          },
          td: {
            background: `${c}.100`,
          },
        },
      },
    },
  };
});

const tableTheme = defineMultiStyleConfig({
  variants: { striped: variantRounded },
});

const theme = extendTheme({
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
