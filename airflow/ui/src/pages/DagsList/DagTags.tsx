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
import { Flex, Text, Tooltip, VStack } from "@chakra-ui/react";
import { FiTag } from "react-icons/fi";

import type { DagTagPydantic } from "openapi/requests/types.gen";

const MAX_TAGS = 3;

type Props = {
  readonly hideIcon?: boolean;
  readonly tags: Array<DagTagPydantic>;
};

export const DagTags = ({ hideIcon = false, tags }: Props) =>
  tags.length ? (
    <Flex alignItems="center" ml={2}>
      {hideIcon ? undefined : <FiTag data-testid="dag-tag" />}
      <Text ml={1}>
        {tags
          .slice(0, MAX_TAGS)
          .map(({ name }) => name)
          .join(", ")}
      </Text>
      {tags.length > MAX_TAGS && (
        <Tooltip
          hasArrow
          label={
            <VStack p={1} spacing={1}>
              {tags.slice(MAX_TAGS).map((tag) => (
                <Text key={tag.name}>{tag.name}</Text>
              ))}
            </VStack>
          }
        >
          <Text>, +{tags.length - MAX_TAGS} more</Text>
        </Tooltip>
      )}
    </Flex>
  ) : undefined;
