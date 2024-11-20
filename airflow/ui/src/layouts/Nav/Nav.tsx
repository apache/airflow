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
import { Box, Flex, VStack, Link } from "@chakra-ui/react";
import { FiCornerUpLeft, FiDatabase, FiHome, FiSettings } from "react-icons/fi";

import { useVersionServiceGetVersion } from "openapi/queries";
import { AirflowPin } from "src/assets/AirflowPin";
import { DagIcon } from "src/assets/DagIcon";

import { AdminButton } from "./AdminButton";
import { BrowseButton } from "./BrowseButton";
import { DocsButton } from "./DocsButton";
import { NavButton } from "./NavButton";
import { UserSettingsButton } from "./UserSettingsButton";

export const Nav = () => {
  const { data } = useVersionServiceGetVersion();

  return (
    <VStack
      alignItems="center"
      bg="blue.muted"
      height="100%"
      justifyContent="space-between"
      left={0}
      position="fixed"
      py={3}
      top={0}
      width={20}
      zIndex={1}
    >
      <Flex alignItems="center" flexDir="column" width="100%">
        <Box mb={3}>
          <AirflowPin height="35px" width="35px" />
        </Box>
        <NavButton icon={<FiHome size="1.75rem" />} title="Home" to="/" />
        <NavButton
          icon={<DagIcon height="1.75rem" width="1.75rem" />}
          title="Dags"
          to="dags"
        />
        <NavButton
          disabled
          icon={<FiDatabase size="1.75rem" />}
          title="Assets"
          to="assets"
        />
        <BrowseButton />
        <AdminButton />
      </Flex>
      <Flex flexDir="column">
        <NavButton
          icon={<FiCornerUpLeft size="1.75rem" />}
          title="Legacy UI"
          to={import.meta.env.VITE_LEGACY_API_URL}
        />
        <DocsButton />
        <UserSettingsButton />
        <Link
          aria-label={data?.version}
          color="fg.info"
          href={`https://airflow.apache.org/docs/apache-airflow/${data?.version}/index.html`}
          rel="noopener noreferrer"
          target="_blank"
        >
          {data?.version}
        </Link>
      </Flex>
    </VStack>
  );
};