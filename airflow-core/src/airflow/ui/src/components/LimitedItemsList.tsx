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
import { Box, Button, Text, HStack, Stack } from "@chakra-ui/react";
import React, { type ReactNode } from "react";
import { useTranslation } from "react-i18next";

import { Popover } from "./ui";

type ListProps = {
  readonly icon?: ReactNode;
  readonly interactive?: boolean;
  readonly items: Array<ReactNode | string>;
  readonly maxItems?: number;
  readonly separator?: string;
};

export const LimitedItemsList = ({
  icon,
  interactive = false,
  items,
  maxItems,
  separator = ", ",
}: ListProps) => {
  const { t: translate } = useTranslation("components");
  const shouldTruncate = maxItems !== undefined && items.length > maxItems;
  const displayItems = shouldTruncate ? items.slice(0, maxItems) : items;
  const remainingItems = shouldTruncate ? items.slice(maxItems) : [];

  if (!items.length) {
    return undefined;
  }

  return (
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
          ) : (
            <Popover.Root lazyMount unmountOnExit>
              <Popover.Trigger asChild>
                <Button
                  colorPalette="brand"
                  cursor="pointer"
                  fontSize="sm"
                  minH="auto"
                  px={1}
                  py={0}
                  size="xs"
                  variant="ghost"
                >
                  {translate("limitedList", { count: remainingItems.length })}
                </Button>
              </Popover.Trigger>
              <Popover.Content maxW="400px" width="fit-content">
                <Popover.Arrow />
                <Popover.Body>
                  <Text fontSize="sm" fontWeight="medium" mb={3}>
                    {translate("limitedList.allItems", { count: items.length })}
                  </Text>

                  <Box maxH="300px" overflowY="auto">
                    {interactive ? (
                      <HStack flexWrap="wrap" gap={2}>
                        {items.map((item, index) => (
                          <Box
                            bg="bg.subtle"
                            borderRadius="sm"
                            key={typeof item === "string" ? item : index}
                            px={2}
                            py={1}
                          >
                            {item}
                          </Box>
                        ))}
                      </HStack>
                    ) : (
                      <Stack gap={1}>
                        {items.map((item, index) => (
                          <Text fontSize="sm" key={typeof item === "string" ? item : index} userSelect="text">
                            {item}
                          </Text>
                        ))}
                      </Stack>
                    )}
                  </Box>

                  <Text fontSize="xs" mt={3}>
                    {interactive
                      ? translate("limitedList.clickToInteract")
                      : translate("limitedList.copyPasteText")}
                  </Text>
                </Popover.Body>
              </Popover.Content>
            </Popover.Root>
          )
        ) : undefined}
      </Box>
    </HStack>
  );
};
