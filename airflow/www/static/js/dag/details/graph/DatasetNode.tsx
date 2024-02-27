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
import {
  Box,
  Link,
  Popover,
  PopoverArrow,
  PopoverBody,
  PopoverCloseButton,
  PopoverContent,
  PopoverHeader,
  PopoverTrigger,
  Portal,
  Text,
} from "@chakra-ui/react";
import { HiDatabase } from "react-icons/hi";
import type { NodeProps } from "reactflow";

import { getMetaValue } from "src/utils";
import { useContainerRef } from "src/context/containerRef";
import type { CustomNodeProps } from "./Node";

const datasetsUrl = getMetaValue("datasets_url");

const DatasetNode = ({
  data: { label, height, width, latestDagRunId, isZoomedOut },
}: NodeProps<CustomNodeProps>) => {
  const containerRef = useContainerRef();

  return (
    <Popover>
      <PopoverTrigger>
        <Box
          borderRadius={isZoomedOut ? 10 : 5}
          borderWidth={1}
          borderColor="gray.400"
          bg="white"
          height={`${height}px`}
          width={`${width}px`}
          cursor={latestDagRunId ? "cursor" : "default"}
          data-testid="node"
          px={isZoomedOut ? 1 : 2}
          mt={isZoomedOut ? -2 : 0}
        >
          <Text
            fontWeight="bold"
            mt={isZoomedOut ? -2 : 0}
            noOfLines={2}
            fontSize={isZoomedOut ? 24 : undefined}
            textAlign="justify"
          >
            {label}
          </Text>
          {!isZoomedOut && (
            <Text
              maxWidth={`calc(${width}px - 12px)`}
              fontWeight={400}
              fontSize="md"
              textAlign="justify"
              color="gray.500"
            >
              <HiDatabase
                size="16px"
                style={{
                  display: "inline",
                  verticalAlign: "middle",
                  marginRight: "3px",
                }}
              />
              Dataset
            </Text>
          )}
        </Box>
      </PopoverTrigger>
      <Portal containerRef={containerRef}>
        <PopoverContent bg="gray.100">
          <PopoverArrow bg="gray.100" />
          <PopoverCloseButton />
          <PopoverHeader>{label}</PopoverHeader>
          <PopoverBody>
            <Link
              color="blue"
              href={`${datasetsUrl}?uri=${encodeURIComponent(label)}`}
            >
              View Dataset
            </Link>
          </PopoverBody>
        </PopoverContent>
      </Portal>
    </Popover>
  );
};

export default DatasetNode;
