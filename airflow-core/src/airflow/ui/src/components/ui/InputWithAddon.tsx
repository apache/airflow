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
import type { InputProps } from "@chakra-ui/react";
import { Box, Input, Text } from "@chakra-ui/react";
import * as React from "react";

export type InputWithAddonProps = {
  readonly label: string;
  readonly width?: string;
} & InputProps;

export const InputWithAddon = React.forwardRef<HTMLInputElement, InputWithAddonProps>((props, ref) => {
  const { label, width = "220px", ...inputProps } = props;

  return (
    <Box
      alignItems="center"
      bg="bg"
      border="1px solid"
      borderColor="border"
      borderRadius="full"
      display="flex"
      width={width}
    >
      <Text
        bg="gray.muted"
        borderLeftRadius="full"
        color="colorPalette.fg"
        colorPalette="gray"
        fontSize="sm"
        fontWeight="medium"
        px={3}
        py={2}
        whiteSpace="nowrap"
      >
        {label}:
      </Text>
      <Input
        bg="transparent"
        border="none"
        borderRadius="0"
        flex="1"
        outline="none"
        ref={ref}
        size="sm"
        {...inputProps}
      />
    </Box>
  );
});

InputWithAddon.displayName = "InputWithAddon";
