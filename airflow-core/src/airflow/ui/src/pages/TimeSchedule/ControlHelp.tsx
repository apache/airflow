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
import type { ReactNode } from "react";

import { Text, VStack } from "@chakra-ui/react";
import { FiInfo } from "react-icons/fi";

import { IconButton, Tooltip } from "src/system-components";

type ControlHelpProps = {
  readonly children: ReactNode;
  readonly label: string;
  readonly title: string;
};

export const ControlHelp = ({ children, label, title }: ControlHelpProps) => (
  <Tooltip
    content={
      <VStack align="start" gap={1.5} maxWidth="xs">
        <Text fontSize="sm" fontWeight="semibold">
          {title}
        </Text>
        {children}
      </VStack>
    }
    portalled
  >
    <IconButton aria-label={label} size="sm">
      <FiInfo />
    </IconButton>
  </Tooltip>
);
