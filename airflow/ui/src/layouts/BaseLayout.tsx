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
import { Flex } from "@chakra-ui/react";
import { useEffect, type PropsWithChildren } from "react";
import { Outlet, useSearchParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import { useConfig } from "src/queries/useConfig";

import { Nav } from "./Nav";

export const TOKEN_STORAGE_KEY = "token";

export const TOKEN_QUERY_PARAM_NAME = "token";

export const BaseLayout = ({ children }: PropsWithChildren) => {
  const instanceName = useConfig("instance_name");
  // const instanceNameHasMarkup =
  //   webserverConfig?.options.find(
  //     ({ key }) => key === "instance_name_has_markup",
  //   )?.value === "True";

  if (typeof instanceName === "string") {
    document.title = instanceName;
  }

  const [searchParams, setSearchParams] = useSearchParams();
  const paramToken = searchParams.get(TOKEN_QUERY_PARAM_NAME);

  const [, setToken] = useLocalStorage<string | null>(TOKEN_STORAGE_KEY, paramToken);

  useEffect(() => {
    if (paramToken !== null) {
      setToken(paramToken);
      searchParams.delete(TOKEN_QUERY_PARAM_NAME);
      setSearchParams(searchParams);
    }
  }, [paramToken, searchParams, setSearchParams, setToken]);

  return (
    <>
      <Nav />
      <Flex flexFlow="column" height="100%" ml={20} p={3}>
        {children ?? <Outlet />}
      </Flex>
    </>
  );
};
