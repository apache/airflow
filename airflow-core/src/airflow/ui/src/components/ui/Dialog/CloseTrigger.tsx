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
import { CloseButton, Dialog as ChakraDialog, type CloseButtonProps } from "@chakra-ui/react";
import { forwardRef } from "react";

type Props = {
  readonly closeButtonProps?: CloseButtonProps;
} & ChakraDialog.CloseTriggerProps;

export const CloseTrigger = forwardRef<HTMLButtonElement, Props>(
  ({ children, closeButtonProps, ...rest }, ref) => (
    <ChakraDialog.CloseTrigger {...rest} asChild>
      <CloseButton
        _hover={{ bg: "brand.emphasized", color: "fg" }}
        borderRadius="md"
        color="fg.muted"
        colorPalette="brand"
        ref={ref}
        size="sm"
        transition="background-color 0.2s ease, color 0.2s ease"
        variant="plain"
        {...closeButtonProps}
      >
        {children}
      </CloseButton>
    </ChakraDialog.CloseTrigger>
  ),
);
