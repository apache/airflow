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
import { Text, Box, useDisclosure, Heading, Stack } from "@chakra-ui/react";

import { Dialog, Tooltip } from "src/components/ui";

import { capitalize } from "./capitalize";
import { trimText } from "./trimTextFn";

const formatKey = (key: string): string => {
  const formatted = capitalize(key).replaceAll("_", " ");

  return formatted;
};

type TrimTextProps = {
  readonly charLimit?: number;
  readonly isClickable?: boolean;
  readonly onClickContent?: Record<string, boolean | number | string | null>;
  readonly showTooltip?: boolean;
  readonly text: string | null;
};

export const TrimText = ({
  charLimit = 50,
  isClickable = false,
  onClickContent,
  showTooltip = false,
  text,
}: TrimTextProps) => {
  const safeText = text ?? "";
  const { isTrimmed, trimmedText } = trimText(safeText, charLimit);

  const { onClose, onOpen, open } = useDisclosure();

  return (
    <>
      <Tooltip
        content={showTooltip && isTrimmed ? safeText : undefined}
        disabled={!isTrimmed || !showTooltip}
      >
        <Box
          _hover={isClickable ? { textDecoration: "underline" } : undefined}
          as={isClickable ? "button" : "span"}
          color={isClickable ? "fg.info" : undefined}
          cursor={isClickable ? "pointer" : "default"}
          fontWeight={isClickable ? "bold" : undefined}
          onClick={onOpen}
        >
          <Text>{trimmedText}</Text>
        </Box>
      </Tooltip>

      <Dialog.Root onOpenChange={onClose} open={isClickable ? open : undefined} size="xl">
        <Dialog.Content backdrop>
          <Dialog.Header>
            <Heading size="xl">Details</Heading>
          </Dialog.Header>

          <Dialog.CloseTrigger />

          <Dialog.Body>
            <Stack gap={4}>
              {onClickContent ? (
                Object.entries(onClickContent).map(([key, value]) => {
                  const formattedKey = formatKey(key);
                  const isEmpty = value === "" || value === null;

                  return (
                    <Box key={key}>
                      <Text fontWeight="bold" mb={2}>
                        {formattedKey}
                      </Text>
                      <Box
                        bg="gray.subtle"
                        borderRadius="md"
                        maxHeight={200}
                        overflow="auto"
                        overflowWrap="break-word"
                        p={4}
                        whiteSpace="pre-wrap"
                      >
                        <Text
                          color={isEmpty ? "gray.emphasized" : undefined}
                          fontWeight={isEmpty ? "bold" : "normal"}
                        >
                          {isEmpty ? "Empty" : String(value)}
                        </Text>
                      </Box>
                    </Box>
                  );
                })
              ) : (
                <Text>No content available.</Text>
              )}
            </Stack>
          </Dialog.Body>
        </Dialog.Content>
      </Dialog.Root>
    </>
  );
};
