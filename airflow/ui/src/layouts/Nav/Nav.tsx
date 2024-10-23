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
import { Box, Flex, Icon, useColorModeValue, VStack } from "@chakra-ui/react";
import { motion } from "framer-motion";
import {
  FiBarChart2,
  FiCornerUpLeft,
  FiDatabase,
  FiGlobe,
  FiHome,
  FiSettings,
} from "react-icons/fi";

import { AirflowPin } from "src/assets/AirflowPin";
import { DagIcon } from "src/assets/DagIcon";

import { DocsButton } from "./DocsButton";
import { NavButton } from "./NavButton";
import { UserSettingsButton } from "./UserSettingsButton";

export const Nav = () => {
  const navBg = useColorModeValue("blue.100", "blue.900");

  return (
    <VStack
      alignItems="center"
      bg={navBg}
      height="100%"
      justifyContent="space-between"
      left={0}
      position="fixed"
      py={3}
      top={0}
      width={24}
      zIndex={1}
    >
      <Flex alignItems="center" flexDir="column" width="100%">
        <Box
          as={motion.div}
          mb={3}
          whileHover={{
            transform: ["rotate(0)", "rotate(360deg)"],
            transition: { duration: 1.5, ease: "linear", repeat: Infinity },
          }}
        >
          <Icon as={AirflowPin} height="35px" width="35px" />
        </Box>
        <NavButton icon={<FiHome size="1.75rem" />} title="Home" to="/" />
        <NavButton
          icon={<DagIcon height={7} width={7} />}
          title="Dags"
          to="dags"
        />
        <NavButton
          icon={<FiDatabase size="1.75rem" />}
          isDisabled
          title="Assets"
          to="assets"
        />
        <NavButton
          icon={<FiBarChart2 size="1.75rem" />}
          isDisabled
          title="Dag Runs"
          to="dag_runs"
        />
        <NavButton
          icon={<FiGlobe size="1.75rem" />}
          isDisabled
          title="Browse"
          to="browse"
        />
        <NavButton
          icon={<FiSettings size="1.75rem" />}
          isDisabled
          title="Admin"
          to="admin"
        />
      </Flex>
      <Flex flexDir="column">
        <NavButton
          icon={<FiCornerUpLeft size="1.5rem" />}
          title="Return to legacy UI"
          to={import.meta.env.VITE_LEGACY_API_URL}
        />
        <DocsButton />
        <UserSettingsButton />
      </Flex>
    </VStack>
  );
};
