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
import { Badge, Box, Button, Code, Heading, HStack, Skeleton, Text, VStack } from "@chakra-ui/react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FiFileText } from "react-icons/fi";

import { useDeadlinesServiceGetCallbackLogs } from "openapi/queries";
import { ErrorAlert } from "src/components/ErrorAlert";
import { Dialog } from "src/components/ui";
import { parseStreamingLogContent } from "src/utils/logs";

type CallbackLogViewerProps = {
  readonly callbackId: string;
  readonly callbackState?: string | null;
  readonly dagId: string;
  readonly dagRunId: string;
};

const stateColorMap: Record<string, string> = {
  failed: "red",
  running: "blue",
  success: "green",
};

export const CallbackLogViewer = ({ callbackId, callbackState, dagId, dagRunId }: CallbackLogViewerProps) => {
  const { t: translate } = useTranslation("dag");
  const [isOpen, setIsOpen] = useState(false);

  const { data, error, isLoading } = useDeadlinesServiceGetCallbackLogs(
    {
      callbackId,
      dagId,
      dagRunId,
    },
    undefined,
    { enabled: isOpen },
  );

  const logContent = parseStreamingLogContent(data);
  const logLines = logContent.map((entry) => (typeof entry === "string" ? entry : entry.event));

  return (
    <>
      <Button onClick={() => setIsOpen(true)} size="xs" variant="outline">
        <FiFileText />
        {translate("callbackLogs.viewLogs")}
      </Button>
      <Dialog.Root onOpenChange={() => setIsOpen(false)} open={isOpen} scrollBehavior="inside" size="lg">
        <Dialog.Content backdrop p={4}>
          <Dialog.Header>
            <HStack gap={2}>
              <Heading size="sm">{translate("callbackLogs.title")}</Heading>
              {callbackState !== undefined && callbackState !== null ? (
                <Badge colorPalette={stateColorMap[callbackState] ?? "gray"} size="sm" variant="solid">
                  {callbackState}
                </Badge>
              ) : undefined}
            </HStack>
          </Dialog.Header>
          <Dialog.CloseTrigger />
          <Dialog.Body pb={2}>
            <ErrorAlert error={error} />
            {isLoading ? (
              <VStack>
                {Array.from({ length: 5 }).map((_, idx) => (
                  // eslint-disable-next-line react/no-array-index-key
                  <Skeleton height="20px" key={idx} width="100%" />
                ))}
              </VStack>
            ) : logLines.length === 0 ? (
              <Text color="fg.muted" fontSize="sm">
                {translate("callbackLogs.noLogs")}
              </Text>
            ) : (
              <Box bg="bg.subtle" borderRadius="md" maxHeight="400px" overflow="auto" p={3} width="100%">
                <Code display="block" fontSize="xs" whiteSpace="pre-wrap" width="100%">
                  {logLines.join("\n")}
                </Code>
              </Box>
            )}
          </Dialog.Body>
        </Dialog.Content>
      </Dialog.Root>
    </>
  );
};
