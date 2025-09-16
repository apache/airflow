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
import { Box, Text, HStack, useDisclosure, Heading, Stack } from "@chakra-ui/react";
import React, { type ReactNode } from "react";
import { useTranslation } from "react-i18next";

import { Tooltip, Dialog, Button } from "./ui";

type ListProps = {
  readonly icon?: ReactNode;
  readonly interactive?: boolean;
  readonly items: Array<ReactNode | string>;
  readonly maxItems?: number;
  readonly modalTitle?: string;
  readonly separator?: string;
  readonly showModal?: boolean;
};

export const LimitedItemsList = ({
  icon,
  interactive = false,
  items,
  maxItems,
  modalTitle = "All Items",
  separator = ", ",
  showModal = false,
}: ListProps) => {
  const { t: translate } = useTranslation("components");
  const { onClose, onOpen, open } = useDisclosure();
  const shouldTruncate = maxItems !== undefined && items.length > maxItems;
  const displayItems = shouldTruncate ? items.slice(0, maxItems) : items;
  const remainingItems = shouldTruncate ? items.slice(maxItems) : [];
  const remainingItemsList = interactive ? (
    <Box maxH="200px" overflowY="auto" p={2}>
      <Text fontSize="sm" fontWeight="bold" mb={2}>
        {translate("limitedList.allItems", { count: items.length })}
      </Text>
      <Stack gap={1}>
        {items.map((item, index) => (
          <Box fontSize="sm" key={typeof item === "string" ? item : index}>
            {item}
          </Box>
        ))}
      </Stack>
      <Text color="gray.500" fontSize="xs" mt={2}>
        {translate("limitedList.clickToOpenFull", { count: remainingItems.length })}
      </Text>
    </Box>
  ) : (
    <Box maxH="200px" overflowY="auto" p={2}>
      <Text fontSize="sm" fontWeight="bold" mb={2}>
        {translate("limitedList.allItems", { count: items.length })}
      </Text>
      <Stack gap={1}>
        {items.map((item, index) => (
          <Text fontSize="sm" key={typeof item === "string" ? item : index}>
            {item}
          </Text>
        ))}
      </Stack>
      {showModal ? (
        <Text color="gray.500" fontSize="xs" mt={2}>
          {translate("limitedList.clickToOpenFull", { count: remainingItems.length })}
        </Text>
      ) : undefined}
    </Box>
  );

  if (!items.length) {
    return undefined;
  }

  return (
    <>
      <HStack align="center" gap={1}>
        {icon}
        <Box fontSize="sm">
          {displayItems.map((item, index) => (
            // eslint-disable-next-line react/no-array-index-key
            <React.Fragment key={index}>
              <Text as="span">{item}</Text>
              {index < displayItems.length - 1 ||
              (shouldTruncate && remainingItems.length >= 1 && index === displayItems.length - 1) ? (
                <Text as="span">{separator}</Text>
              ) : undefined}
            </React.Fragment>
          ))}
          {shouldTruncate ? (
            remainingItems.length === 1 ? (
              <Text as="span">{remainingItems[0]}</Text>
            ) : showModal ? (
              <Tooltip content={remainingItemsList} interactive={interactive}>
                <Button
                  _hover={{ color: "blue.600", textDecoration: "underline" }}
                  color="blue.500"
                  cursor="pointer"
                  fontSize="sm"
                  minH="auto"
                  onClick={onOpen}
                  px={1}
                  py={0}
                  size="xs"
                  variant="ghost"
                >
                  {translate("limitedList", { count: remainingItems.length })}
                </Button>
              </Tooltip>
            ) : (
              <Tooltip content={remainingItemsList} interactive={interactive}>
                <Text as="span" cursor="help">
                  {translate("limitedList", { count: remainingItems.length })}
                </Text>
              </Tooltip>
            )
          ) : undefined}
        </Box>
      </HStack>

      {/* Modal for showing all items */}
      {showModal ? (
        <Dialog.Root onOpenChange={onClose} open={open} size="xl">
          <Dialog.Content backdrop>
            <Dialog.Header>
              <Heading size="lg">{modalTitle}</Heading>
            </Dialog.Header>

            <Dialog.CloseTrigger />

            <Dialog.Body>
              <Box>
                <Text color="gray.600" fontSize="sm" mb={3}>
                  {translate("limitedList.showingItems", { count: items.length })}
                </Text>

                <Box
                  bg="bg.subtle"
                  border="1px solid"
                  borderColor="border.subtle"
                  borderRadius="md"
                  maxH="400px"
                  overflowY="auto"
                  p={3}
                >
                  {interactive ? (
                    <HStack flexWrap="wrap" gap={2}>
                      {items.map((item, index) => (
                        <Box key={typeof item === "string" ? item : index}>{item}</Box>
                      ))}
                    </HStack>
                  ) : (
                    <Stack gap={2}>
                      {items.map((item, index) => (
                        <Text fontSize="sm" key={typeof item === "string" ? item : index} userSelect="text">
                          {item}
                        </Text>
                      ))}
                    </Stack>
                  )}
                </Box>

                <Text color="gray.500" fontSize="xs" mt={3}>
                  {interactive
                    ? translate("limitedList.clickToInteract")
                    : translate("limitedList.copyPasteText")}
                </Text>
              </Box>
            </Dialog.Body>
          </Dialog.Content>
        </Dialog.Root>
      ) : undefined}
    </>
  );
};
