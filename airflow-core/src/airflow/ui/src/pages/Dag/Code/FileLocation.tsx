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
import { Box, Code, HStack } from "@chakra-ui/react";

import { ClipboardIconButton, ClipboardRoot, Tooltip } from "src/components/ui";

type FileLocationProps = {
  readonly fileloc: string;
  readonly relativeFileloc: string | null;
};

export const FileLocation = ({ fileloc, relativeFileloc }: FileLocationProps) => {
  const displayFilename =
    relativeFileloc !== null && relativeFileloc !== ""
      ? relativeFileloc
      : (fileloc.split("/").at(-1) ?? fileloc);

  return (
    <Box
      bg="bg.subtle"
      borderBottomWidth={1}
      borderColor="border.emphasized"
      borderTopRadius={8}
      px={3}
      py={1}
    >
      <HStack gap={2}>
        <Tooltip closeDelay={100} content={fileloc} openDelay={100}>
          <Code fontSize="13px" wordBreak="break-word">
            {displayFilename}
          </Code>
        </Tooltip>
        <ClipboardRoot value={fileloc}>
          <ClipboardIconButton size="2xs" />
        </ClipboardRoot>
      </HStack>
    </Box>
  );
};
