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
  const { colorMode, setColorMode } = useColorMode();

  return (
    <Box p={8} bg="bg.subtle" flexGrow={1} height="100%">
      <VStack gap={8} align="center" justify="center" flexGrow={1} height="100%">
        <Heading size="2xl" textAlign="center" color="fg">
          Welcome to Your New React App!
        </Heading>
        <Text fontSize="lg" color="fg.muted">
          This project was bootstrapped with the Airflow React Plugin tool.
        </Text>
        <Button onClick={() => setColorMode(colorMode === "dark" ? "light" : "dark")} colorPalette="brand">
          Toggle Theme
        </Button>
      </VStack>
    </Box>
  );
};
