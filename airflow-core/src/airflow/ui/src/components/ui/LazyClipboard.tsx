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
import type { ButtonProps } from "@chakra-ui/react";
import { IconButton } from "@chakra-ui/react";
import * as React from "react";
import { LuCheck, LuClipboard } from "react-icons/lu";

type LazyClipboardProps = {
  readonly getValue: () => string;
} & ButtonProps;

/** Clipboard button that lazily computes the value only when clicked */
export const LazyClipboard = React.forwardRef<HTMLButtonElement, LazyClipboardProps>(
  ({ getValue, ...props }, ref) => {
    const [copied, setCopied] = React.useState(false);

    const handleClick = () => {
      const value = getValue();

      void navigator.clipboard.writeText(value);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    };

    return (
      <IconButton onClick={handleClick} ref={ref} size="xs" variant="subtle" {...props}>
        {copied ? <LuCheck /> : <LuClipboard />}
      </IconButton>
    );
  },
);
