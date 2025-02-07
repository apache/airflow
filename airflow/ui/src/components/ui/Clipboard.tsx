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
import type { ButtonProps, InputProps } from "@chakra-ui/react";
import { Button, Clipboard as ChakraClipboard, IconButton, Input } from "@chakra-ui/react";
import * as React from "react";
import { LuCheck, LuClipboard, LuLink } from "react-icons/lu";

const ClipboardIcon = React.forwardRef<HTMLDivElement, ChakraClipboard.IndicatorProps>((props, ref) => (
  <ChakraClipboard.Indicator copied={<LuCheck />} {...props} ref={ref}>
    <LuClipboard />
  </ChakraClipboard.Indicator>
));

const ClipboardCopyText = React.forwardRef<HTMLDivElement, ChakraClipboard.IndicatorProps>((props, ref) => (
  <ChakraClipboard.Indicator copied="Copied" {...props} ref={ref}>
    Copy
  </ChakraClipboard.Indicator>
));

export const ClipboardLabel = React.forwardRef<HTMLLabelElement, ChakraClipboard.LabelProps>((props, ref) => (
  <ChakraClipboard.Label
    display="inline-block"
    fontWeight="medium"
    mb="1"
    textStyle="sm"
    {...props}
    ref={ref}
  />
));

export const ClipboardButton = React.forwardRef<HTMLButtonElement, ButtonProps>((props, ref) => (
  <ChakraClipboard.Trigger asChild>
    <Button ref={ref} size="sm" variant="surface" {...props}>
      <ClipboardIcon />
      <ClipboardCopyText />
    </Button>
  </ChakraClipboard.Trigger>
));

export const ClipboardLink = React.forwardRef<HTMLButtonElement, ButtonProps>((props, ref) => (
  <ChakraClipboard.Trigger asChild>
    <Button
      alignItems="center"
      display="inline-flex"
      gap="2"
      ref={ref}
      size="xs"
      unstyled
      variant="plain"
      {...props}
    >
      <LuLink />
      <ClipboardCopyText />
    </Button>
  </ChakraClipboard.Trigger>
));

export const ClipboardIconButton = React.forwardRef<HTMLButtonElement, ButtonProps>((props, ref) => (
  <ChakraClipboard.Trigger asChild>
    <IconButton ref={ref} size="xs" variant="subtle" {...props}>
      <ClipboardIcon />
      <ClipboardCopyText srOnly />
    </IconButton>
  </ChakraClipboard.Trigger>
));

export const ClipboardInput = React.forwardRef<HTMLInputElement, InputProps>((props, ref) => (
  <ChakraClipboard.Input asChild>
    <Input ref={ref} {...props} />
  </ChakraClipboard.Input>
));

export const ClipboardRoot = ChakraClipboard.Root;
