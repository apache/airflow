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
import { Center, Flex } from "@chakra-ui/react";
import type { ReactNode } from "react";
import { NavLink, useSearchParams } from "react-router-dom";

type Props = {
  readonly keepSearch?: boolean;
  readonly rightButtons?: ReactNode;
  readonly tabs: Array<{ label: string; value: string }>;
};

export const NavTabs = ({ keepSearch, rightButtons, tabs }: Props) => {
  const [searchParams] = useSearchParams();

  return (
    <Flex alignItems="center" borderBottomWidth={1} justifyContent="space-between" mb={2}>
      <Flex>
        {tabs.map(({ label, value }) => (
          <NavLink
            end
            key={value}
            to={{
              pathname: value,
              // Preserve search params when navigating
              search: keepSearch ? searchParams.toString() : undefined,
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
      <Flex alignSelf="flex-end">{rightButtons}</Flex>
    </Flex>
  );
};
