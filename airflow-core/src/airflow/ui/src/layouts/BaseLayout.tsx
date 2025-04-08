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
import type { PropsWithChildren } from "react";
import { Outlet } from "react-router-dom";

import { useConfig } from "src/queries/useConfig";

import { Nav } from "./Nav";

export const BaseLayout = ({ children }: PropsWithChildren) => {
  const instanceName = useConfig("instance_name");

  if (typeof instanceName === "string") {
    document.title = instanceName;
  }

  return (
    <>
      <Nav />
      <Box display="flex" flexDirection="column" h="100vh" ml={20} p={3}>
        {children ?? <Outlet />}
      </Box>
    </>
  );
};
