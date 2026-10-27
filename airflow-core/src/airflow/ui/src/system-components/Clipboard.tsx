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
import { forwardRef } from "react";

import type { ButtonProps, InputProps } from "@chakra-ui/react";
import { Clipboard as ChakraClipboard, Input } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { LuCheck, LuClipboard } from "react-icons/lu";

import { IconButton } from ".";

const ClipboardIcon = forwardRef<HTMLDivElement, ChakraClipboard.IndicatorProps>((props, ref) => (
  <ChakraClipboard.Indicator copied={<LuCheck />} {...props} ref={ref}>
    <LuClipboard />
  </ChakraClipboard.Indicator>
));

const ClipboardCopyText = forwardRef<HTMLDivElement, ChakraClipboard.IndicatorProps>((props, ref) => {
  const { t: translate } = useTranslation("components");

  return (
    <ChakraClipboard.Indicator copied="Copied" {...props} ref={ref}>
      {translate("clipboard.copy")}
    </ChakraClipboard.Indicator>
  );
});

export const ClipboardIconButton = forwardRef<HTMLButtonElement, ButtonProps>((props, ref) => (
  <ChakraClipboard.Trigger asChild>
    <IconButton ref={ref} size="xs" variant="ghost" {...props}>
      <ClipboardIcon />
      <ClipboardCopyText srOnly />
    </IconButton>
  </ChakraClipboard.Trigger>
));

export const ClipboardInput = forwardRef<HTMLInputElement, InputProps>((props, ref) => (
  <ChakraClipboard.Input asChild>
    <Input ref={ref} {...props} />
  </ChakraClipboard.Input>
));

export const ClipboardRoot = ChakraClipboard.Root;
