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
import { Status as ChakraStatus } from "@chakra-ui/react";
import * as React from "react";

import type {
  DagRunState,
  TaskInstanceState,
} from "openapi/requests/types.gen";
import { stateColor } from "src/utils/stateColor";

type StatusValue = DagRunState | TaskInstanceState;

export type StatusProps = {
  state?: StatusValue;
} & ChakraStatus.RootProps;

export const Status = React.forwardRef<HTMLDivElement, StatusProps>(
  ({ children, state, ...rest }, ref) => {
    const colorPalette = state === undefined ? "info" : stateColor[state];

    return (
      <ChakraStatus.Root ref={ref} {...rest}>
        <ChakraStatus.Indicator bg={colorPalette} />
        {children}
      </ChakraStatus.Root>
    );
  },
);
