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

import { ReactElement } from "react";
import { Box, Button, ButtonProps } from "@chakra-ui/react";
import { Link as RouterLink } from "react-router-dom";

type NavButtonProps = {
  title?: string;
  icon: ReactElement;
  href?: string;
  to?: string;
} & ButtonProps;

export const NavButton = ({ icon, title, to, ...rest }: NavButtonProps) => (
  <Button
    as={RouterLink}
    to={to}
    variant="ghost"
    borderRadius="none"
    height={16}
    alignItems="center"
    flexDir="column"
    whiteSpace="wrap"
    width={24}
    transition="0.2s background-color ease-in-out"
    {...rest}
  >
    <Box alignSelf="center">{icon}</Box>
    <Box fontSize="xs">{title}</Box>
  </Button>
);
