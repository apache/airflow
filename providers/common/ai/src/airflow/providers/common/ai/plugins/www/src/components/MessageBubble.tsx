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

import { Box, Text, VStack } from "@chakra-ui/react";
import type { FC } from "react";

import { Markdown } from "src/components/Markdown";
import type { ConversationEntry } from "src/types/feedback";

interface MessageBubbleProps {
  entry: ConversationEntry;
}

export const MessageBubble: FC<MessageBubbleProps> = ({ entry }) => {
  const isHuman = entry.role === "human";
  const label = isHuman ? "You" : "AI Assistant";

  return (
    <Box
      alignSelf={isHuman ? "flex-end" : "flex-start"}
      bg={isHuman ? "brand.solid" : "bg.subtle"}
      borderRadius="xl"
      borderBottomLeftRadius={isHuman ? "xl" : "sm"}
      borderBottomRightRadius={isHuman ? "sm" : "xl"}
      boxShadow="sm"
      color={isHuman ? "white" : "fg"}
      fontSize="sm"
      lineHeight="tall"
      maxW="80%"
      p={4}
    >
      <VStack align={isHuman ? "flex-end" : "flex-start"} gap={1}>
        <Text
          color={isHuman ? "whiteAlpha.800" : "fg.muted"}
          fontSize="xs"
          fontWeight="semibold"
          textTransform="uppercase"
          letterSpacing="wider"
        >
          {label}
        </Text>
        <Box width="full">
          <Markdown content={entry.content} />
        </Box>
        <Text
          color={isHuman ? "whiteAlpha.600" : "fg.muted"}
          fontSize="2xs"
          mt={1}
        >
          Iteration {entry.iteration}
        </Text>
      </VStack>
    </Box>
  );
};
