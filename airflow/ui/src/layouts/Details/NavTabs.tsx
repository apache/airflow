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
import { Button, Center, Flex } from "@chakra-ui/react";
import { FaChartGantt } from "react-icons/fa6";
import { FiGrid } from "react-icons/fi";
import { NavLink, Link as RouterLink } from "react-router-dom";

import { DagIcon } from "src/assets/DagIcon";

type Props = {
  readonly tabs: Array<{ label: string; value: string }>;
};

export const NavTabs = ({ tabs }: Props) => (
  <Flex
    alignItems="center"
    borderBottomWidth={1}
    justifyContent="space-between"
  >
    <Flex>
      {tabs.map(({ label, value }) => (
        <NavLink end key={value} to={value}>
          {({ isActive }) => (
            <Center
              borderBottomColor="border.info"
              borderBottomWidth={isActive ? 3 : 0}
              color={isActive ? "fg" : "fg.muted"}
              fontWeight="bold"
              height="40px"
              mb="-2px" // Show the border on top of its parent's border
              pb={isActive ? 0 : "3px"}
              px={4}
              transition="all 0.2s ease"
            >
              {label}
            </Center>
          )}
        </NavLink>
      ))}
    </Flex>
    <Flex alignSelf="flex-end">
      <Button asChild colorPalette="blue" variant="ghost">
        <RouterLink to={{ search: "modal=gantt" }}>
          <FaChartGantt height={5} width={5} />
          Gantt
        </RouterLink>
      </Button>
      <Button asChild colorPalette="blue" variant="ghost">
        <RouterLink to={{ search: "modal=grid" }}>
          <FiGrid height={5} width={5} />
          Grid
        </RouterLink>
      </Button>
      <Button asChild colorPalette="blue" variant="ghost">
        <RouterLink to={{ search: "modal=graph" }}>
          <DagIcon height={5} width={5} />
          Graph
        </RouterLink>
      </Button>
    </Flex>
  </Flex>
);
