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

import { Box, Code, Text } from "@chakra-ui/react";
import type { FC } from "react";

export const NoSession: FC = () => {
  return (
    <Box
      alignItems="center"
      bg="bg"
      color="fg"
      display="flex"
      h="100%"
      justifyContent="center"
      minH="100vh"
      p={5}
    >
      <Box
        bg="bg.subtle"
        borderRadius="xl"
        borderWidth="1px"
        maxW="440px"
        p={10}
        textAlign="center"
      >
        <Text fontSize="4xl" mb={4}>
          &#x1F4AC;
        </Text>
        <Text as="h2" fontSize="lg" fontWeight="semibold" mb={2}>
          No Active HITL Review Session
        </Text>
        <Text color="fg.muted" fontSize="sm" lineHeight="tall" mb={5}>
          This task does not have an active HITL review session right now. The chat window appears
          when the task is running with <Code fontSize="xs">enable_hitl_review=True</Code>.
        </Text>
        <Box
          bg="bg.subtle"
          borderRadius="lg"
          borderWidth="1px"
          color="fg.muted"
          fontSize="xs"
          lineHeight="tall"
          p={3}
        >
          <Text as="span" opacity={0.8}>
            &#x25CF;
          </Text>{" "}
          If the task is currently running, the session may still be initialising.
        </Box>
      </Box>
    </Box>
  );
};
