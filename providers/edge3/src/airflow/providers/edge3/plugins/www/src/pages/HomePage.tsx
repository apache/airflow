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
import { GrCloudComputer } from "react-icons/gr";

export const HomePage = () => {
  const { toggleColorMode } = useColorMode();

  return (
    <Box p={8} bg="bg.subtle" flexGrow={1} height="100%">
      <VStack gap={8} align="center" justify="center" flexGrow={1} height="100%">
        <Heading size="2xl" textAlign="center" color="fg">
          <GrCloudComputer size={128} />
          Welcome to Edge Executor Plugin!
        </Heading>
        <Text fontSize="lg" color="fg.muted">
          This is a new view for Airflow Edge Executor Plugin for Airflow 3.
        </Text>
        <Button onClick={toggleColorMode} colorPalette="blue">
          Toggle Theme
        </Button>
      </VStack>
    </Box>
  );
};
