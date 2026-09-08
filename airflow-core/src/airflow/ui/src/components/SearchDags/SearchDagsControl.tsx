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
import { Box } from "@chakra-ui/react";
import { chakraComponents } from "chakra-react-select";
import type { ControlProps } from "chakra-react-select";
import { FiSearch } from "react-icons/fi";

import type { DagSearchOption } from "src/utils/option";

/**
 * Leads the input with the search affordance. react-select only renders indicators after the value
 * container, so an icon on the start side has to come from the control itself.
 */
export const Control = ({ children, ...props }: ControlProps<DagSearchOption, false>) => (
  <chakraComponents.Control {...props}>
    <Box alignItems="center" as="span" color="fg.muted" display="flex" flexShrink={0} pe={1.5}>
      <FiSearch />
    </Box>
    {children}
  </chakraComponents.Control>
);
