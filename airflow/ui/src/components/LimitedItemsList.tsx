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
import { Flex, Text, VStack } from "@chakra-ui/react";
import type { ReactNode } from "react";

import { Tooltip } from "src/components/ui";

type Props = {
  readonly icon?: ReactNode;
  readonly items: Array<string>;
  readonly maxItems?: number;
  readonly wrap?: boolean;
};

export const LimitedItemsList = ({ icon, items, maxItems = 3, wrap = false }: Props) =>
  items.length ? (
    <Flex alignItems="center" textWrap={wrap ? "normal" : "nowrap"}>
      {icon}
      <Text fontSize="sm">{items.slice(0, maxItems).join(", ")}</Text>
      {items.length > maxItems && (
        <Tooltip
          content={
            <VStack gap={1} p={1}>
              {items.slice(maxItems).map((item) => (
                <Text key={item}>{item}</Text>
              ))}
            </VStack>
          }
        >
          <Text as="span">, +{items.length - maxItems} more</Text>
        </Tooltip>
      )}
    </Flex>
  ) : undefined;
