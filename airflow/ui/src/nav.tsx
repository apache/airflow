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
  Badge,
  Box,
  Flex,
  Icon,
  IconButton,
  Link,
  Stack,
  Text,
  useColorMode,
} from "@chakra-ui/react";
import { motion } from "framer-motion";
import { MdDarkMode, MdLightMode } from "react-icons/md";

import { AirflowPin } from "./assets/AirflowPin";

export const Nav = () => {
  const { colorMode, toggleColorMode } = useColorMode();
  return (
    <Flex
      p={3}
      boxShadow="base"
      justifyContent="space-between"
      alignContent="center"
    >
      <Flex>
        <Box
          as={motion.div}
          whileHover={{
            transform: ["rotate(0)", "rotate(360deg)"],
            transition: { duration: 1.5, repeat: Infinity, ease: "linear" },
          }}
        >
          <Icon as={AirflowPin} height="35px" width="35px" />
        </Box>
        <Text
          fontFamily="rubik,sans-serif"
          alignSelf="center"
          fontSize="lg"
          ml={2}
        >
          Airflow
        </Text>
      </Flex>
      <Stack direction="row">
        <Badge colorScheme="blue" alignSelf="center">
          <Link href="/">Return to the legacy UI</Link>
        </Badge>
        <IconButton
          onClick={toggleColorMode}
          icon={colorMode === "light" ? <MdDarkMode /> : <MdLightMode />}
          aria-label={`Toggle ${colorMode} mode`}
        />
      </Stack>
    </Flex>
  );
};
