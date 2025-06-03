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
import { Box, Text, HStack, StackSeparator } from "@chakra-ui/react";
import React, { type ReactNode } from "react";

import { Tooltip } from "./ui";

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
  const shouldTruncate = maxItems !== undefined && items.length > maxItems;
  const displayItems = shouldTruncate ? items.slice(0, maxItems) : items;
  const remainingItems = shouldTruncate ? items.slice(maxItems) : [];
  const remainingItemsList = interactive ? (
    <HStack separator={<StackSeparator />}>{remainingItems}</HStack>
  ) : (
    `More items: ${remainingItems.map((item) => (typeof item === "string" ? item : "item")).join(", ")}`
  );

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
            <Tooltip content={remainingItemsList} interactive={interactive}>
              <Text as="span" cursor="help">
                +{remainingItems.length} more
              </Text>
            </Tooltip>
          )
        ) : undefined}
      </Box>
    </HStack>
  );
};
