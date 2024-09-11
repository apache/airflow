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

import { Box, Button, type ButtonProps, Text } from "@chakra-ui/react";

import type { ReactElement } from "react";

type NavButtonProps = {
  readonly href?: string;
  readonly icon: ReactElement;
  readonly title?: string;
} & ButtonProps;

export const NavButton = ({ icon, title, ...rest }: NavButtonProps) => (
  <Button
    alignItems="center"
    borderRadius="none"
    flexDir="column"
    height={16}
    variant="ghost"
    whiteSpace="wrap"
    {...rest}
  >
    <Box alignSelf="center">{icon}</Box>
    <Text fontSize="xs">{title}</Text>
  </Button>
);
