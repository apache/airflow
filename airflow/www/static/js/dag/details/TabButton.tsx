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

import React from "react";
import { Button, ButtonProps } from "@chakra-ui/react";

interface Props extends ButtonProps {
  isActive?: boolean;
}

const TabButton = ({ children, isActive, ...otherProps }: Props) => (
  <Button
    variant="ghost"
    display="flex"
    alignItems="center"
    fontSize="lg"
    py={3}
    pl={4}
    pr={4}
    mt="4px"
    borderRadius={0}
    colorScheme={isActive ? "blue" : undefined}
    borderBottomWidth={isActive ? 2 : 0}
    borderBottomColor={isActive ? "blue.400" : undefined}
    {...otherProps}
  >
    {children}
  </Button>
);

export default TabButton;
