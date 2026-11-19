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
import type { ReactNode } from "react";

import { Box, HStack, Text, VStack } from "@chakra-ui/react";
import { FiAlertCircle, FiAlertTriangle } from "react-icons/fi";

type Props = {
  /** The detail under the heading. Omit for a status that says all it has to say in its heading. */
  readonly body?: ReactNode;
  readonly heading: string;
  readonly severity: "error" | "warning";
};

/**
 * One problem stated in a list of them: its severity in the icon and the colour of its text, its
 * short form as the heading, the long form underneath. Coloured text rather than a filled block,
 * so a list of these stays readable and a long traceback does not become a wall of colour.
 */
export const StatusMessage = ({ body, heading, severity }: Props) => (
  <VStack align="start" gap={1} w="100%">
    <HStack color={`fg.${severity}`} gap={2}>
      {severity === "error" ? <FiAlertCircle /> : <FiAlertTriangle />}
      <Text fontWeight="bold">{heading}</Text>
    </HStack>
    {body === undefined ? undefined : typeof body === "string" ? (
      <Text color={`fg.${severity}`} fontSize="sm" whiteSpace="pre-wrap" wordBreak="break-word">
        {body}
      </Text>
    ) : (
      // Rich detail brings its own blocks, which a <p> may not contain, and its own type scale and
      // colours: a traceback is not the same kind of text as the row naming the file it came from.
      <Box w="100%">{body}</Box>
    )}
  </VStack>
);
