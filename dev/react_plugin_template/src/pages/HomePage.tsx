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

import { Box, Button, Heading, Text, VStack } from "@chakra-ui/react";
import { useColorMode } from "src/context/colorMode";

export const HomePage = () => {
  const { toggleColorMode } = useColorMode();

  return (
    <Box
      minH="100vh"
      p={8}
      bg="gray.50"
      _dark={{ bg: "gray.900" }}
    >
      <VStack spacing={8} align="center" justify="center" minH="100vh">
        <Heading size="2xl" textAlign="center">
          Welcome to Your New React App!
        </Heading>
        <Text fontSize="lg" color="gray.600" _dark={{ color: "gray.400" }}>
          This project was bootstrapped with the Airflow UI configuration.
        </Text>
        <Button onClick={toggleColorMode} colorScheme="blue">
          Toggle Theme
        </Button>
      </VStack>
    </Box>
  );
};
