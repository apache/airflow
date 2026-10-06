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

interface NoModelInfoProps {
  error?: string | null;
}

export const NoModelInfo: FC<NoModelInfoProps> = ({ error }) => {
  return (
    <Box p={5}>
      <Box
        bg="bg.subtle"
        borderRadius="xl"
        borderWidth="1px"
        maxW="440px"
        mx="auto"
        p={10}
        textAlign="center"
      >
        <Text fontSize="4xl" mb={4}>
          &#x1F916;
        </Text>
        <Text as="h2" fontSize="lg" fontWeight="semibold" mb={2}>
          No AI Model Info
        </Text>
        <Text color="fg.muted" fontSize="sm" lineHeight="tall">
          {error ?? (
            <>
              This task did not publish a <Code fontSize="xs">model_name</Code> or{" "}
              <Code fontSize="xs">usage</Code> XCom. The Model tab shows data for tasks run with{" "}
              <Code fontSize="xs">LLMOperator</Code> or <Code fontSize="xs">AgentOperator</Code>.
            </>
          )}
        </Text>
      </Box>
    </Box>
  );
};
