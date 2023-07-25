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

import React from "react";
import { Button, Flex, Link, Box, Text, Divider } from "@chakra-ui/react";

import { useExtraLinks } from "src/api";

interface Props {
  dagId: string;
  taskId: string;
  executionDate: string;
  mapIndex?: number | undefined;
  extraLinks: string[];
}

const ExtraLinks = ({
  dagId,
  taskId,
  executionDate,
  mapIndex,
  extraLinks,
}: Props) => {
  const { data: links } = useExtraLinks({
    dagId,
    taskId,
    executionDate,
    mapIndex,
    extraLinks,
  });

  if (!links?.length) return null;

  const isExternal = (url: string | null) =>
    url && /^(?:[a-z]+:)?\/\//.test(url);

  return (
    <Box mb={3}>
      <Text as="strong">Extra Links</Text>
      <Divider my={2} />
      <Flex flexWrap="wrap">
        {links.map(({ name, url }) => (
          <Button
            key={name}
            as={Link}
            colorScheme="blue"
            href={url}
            isDisabled={!url}
            target={isExternal(url) ? "_blank" : undefined}
            mr={2}
          >
            {name}
          </Button>
        ))}
      </Flex>
      <Divider my={2} />
    </Box>
  );
};

export default ExtraLinks;
