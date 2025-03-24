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
import { Select as ChakraSelect } from "@chakra-ui/react";
import { forwardRef } from "react";

import { CloseButton } from "../CloseButton";

type Props = {
  readonly clearable?: boolean;
  readonly isActive?: boolean;
} & ChakraSelect.ControlProps;

export const Trigger = forwardRef<HTMLButtonElement, Props>((props, ref) => {
  const { children, clearable, isActive, ...rest } = props;

  return (
    <ChakraSelect.Control {...rest}>
      <ChakraSelect.Trigger ref={ref}>{children}</ChakraSelect.Trigger>
      <ChakraSelect.IndicatorGroup>
        {clearable ? (
          <ChakraSelect.ClearTrigger asChild>
            <CloseButton
              focusRingWidth="2px"
              focusVisibleRing="inside"
              pointerEvents="auto"
              size="xs"
              variant="plain"
            />
          </ChakraSelect.ClearTrigger>
        ) : undefined}
        <ChakraSelect.Indicator />
      </ChakraSelect.IndicatorGroup>
    </ChakraSelect.Control>
  );
});
