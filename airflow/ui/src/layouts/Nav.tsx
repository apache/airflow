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

import {
  Box,
  Flex,
  Icon,
  Link,
  useColorMode,
  useColorModeValue,
  VStack,
} from "@chakra-ui/react";
import { motion } from "framer-motion";

import { AirflowPin } from "../assets/AirflowPin";
import {
  FiBarChart2,
  FiCornerUpLeft,
  FiDatabase,
  FiGlobe,
  FiHome,
  FiMoon,
  FiSettings,
  FiSun,
} from "react-icons/fi";
import { DagIcon } from "../assets/DagIcon";
import { NavButton } from "./NavButton";

export const Nav = () => {
  const { colorMode, toggleColorMode } = useColorMode();
  const navBg = useColorModeValue("blue.100", "blue.900");
  return (
    <VStack
      py={3}
      width={24}
      height="100%"
      position="fixed"
      top={0}
      left={0}
      zIndex={1}
      bg={navBg}
      justifyContent="space-between"
      alignItems="center"
    >
      <Flex width="100%" flexDir="column" alignItems="center">
        <Box
          as={motion.div}
          whileHover={{
            transform: ["rotate(0)", "rotate(360deg)"],
            transition: { duration: 1.5, repeat: Infinity, ease: "linear" },
          }}
          mb={3}
        >
          <Icon as={AirflowPin} height="35px" width="35px" />
        </Box>
        <NavButton title="Home" icon={<FiHome size="1.75rem" />} isDisabled />
        <NavButton
          title="DAGs"
          to="dags"
          icon={<DagIcon height={7} width={7} />}
        />
        <NavButton
          title="Datasets"
          icon={<FiDatabase size="1.75rem" />}
          isDisabled
        />
        <NavButton
          title="DAG Runs"
          icon={<FiBarChart2 size="1.75rem" />}
          isDisabled
        />
        <NavButton
          title="Browse"
          icon={<FiGlobe size="1.75rem" />}
          isDisabled
        />
        <NavButton
          title="Admin"
          icon={<FiSettings size="1.75rem" />}
          isDisabled
        />
      </Flex>
      <Flex flexDir="column">
        <NavButton
          title="Return to legacy UI"
          icon={<FiCornerUpLeft size="1.5rem" />}
          as={Link}
          href="/"
        />
        <NavButton
          icon={
            colorMode === "light" ? (
              <FiMoon size="1.75rem" />
            ) : (
              <FiSun size="1.75rem" />
            )
          }
          onClick={toggleColorMode}
        />
      </Flex>
    </VStack>
  );
};
