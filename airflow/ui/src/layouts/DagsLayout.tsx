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
import { Box, Center, Flex } from "@chakra-ui/react";
import type { PropsWithChildren } from "react";
import { NavLink } from "react-router-dom";

const tabs = [
  { label: "Dags", value: "/dags" },
  { label: "Runs", value: "/runs" },
  { label: "Instances", value: "/instances" },
];

export const DagsLayout = ({ children }: PropsWithChildren) => (
  <Box>
    <Flex alignItems="center" borderBottomWidth={1} mb={2}>
      {tabs.map(({ label, value }) => (
        <NavLink
          end
          key={value}
          to={{
            pathname: value,
          }}
        >
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
    {children}
  </Box>
);
