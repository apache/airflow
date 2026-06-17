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
import { IconButton as ChakraIconButton } from "@chakra-ui/react";
import type { IconButtonProps } from "@chakra-ui/react";
import { forwardRef } from "react";

import { Tooltip } from "./Tooltip";

type Props = {
  readonly label?: string;
} & IconButtonProps;

// variant="ghost" is set here since IconButton shares the button recipe with Button.
export const IconButton = forwardRef<HTMLButtonElement, Props>(
  ({ label, variant = "ghost", ...props }, ref) =>
    label === undefined ? (
      <ChakraIconButton ref={ref} variant={variant} {...props} />
    ) : (
      <Tooltip content={label}>
        <ChakraIconButton aria-label={label} ref={ref} variant={variant} {...props} />
      </Tooltip>
    ),
);
