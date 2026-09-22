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
import { useMemo, useState } from "react";

import { Box, Button, Heading, HStack, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiFileText } from "react-icons/fi";

import { useDeadlinesServiceGetCallbackLogs } from "openapi/queries";
import type { CallbackState, TaskInstanceState } from "openapi/requests/types.gen";

import { Modal } from "src/system-components";

import { StateBadge } from "src/components/StateBadge";

import { TaskLogContent } from "src/pages/TaskInstance/Logs/TaskLogContent";
import { parseLogs } from "src/queries/useLogs";
import { parseStreamingLogContent } from "src/utils/logs";

type CallbackLogViewerProps = {
  readonly callbackId: string;
  readonly callbackState?: CallbackState | null;
  readonly dagId: string;
  readonly dagRunId: string;
};

export const CallbackLogViewer = ({ callbackId, callbackState, dagId, dagRunId }: CallbackLogViewerProps) => {
  const { t: translate } = useTranslation(["dag", "common"]);
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

  const parsedData = useMemo(
    () =>
      parseLogs({
        data: parseStreamingLogContent(data),
        showTimestamp: true,
        translate,
        tryNumber: 1,
      }),
    [data, translate],
  );
  const parsedLogs = parsedData.parsedLogs ?? [];

  return (
    <>
      <Button onClick={() => setIsOpen(true)} size="xs" variant="outline">
        <FiFileText />
        {translate("dag:callbackLogs.viewLogs")}
      </Button>
      <Modal
        contentProps={{ backdrop: true, padding: 4 }}
        onOpenChange={() => setIsOpen(false)}
        open={isOpen}
        scrollBehavior="inside"
        size="xl"
        title={
          <HStack gap={2}>
            <Heading size="sm">{translate("dag:callbackLogs.title")}</Heading>
            {callbackState === undefined || callbackState === null ? undefined : (
              <StateBadge size="sm" state={callbackState as TaskInstanceState}>
                {callbackState}
              </StateBadge>
            )}
          </HStack>
        }
      >
        {!isLoading && parsedLogs.length === 0 && error === null ? (
          <Text color="fg.muted" fontSize="sm">
            {translate("dag:callbackLogs.noLogs")}
          </Text>
        ) : (
          <Box display="flex" flexDirection="column" flexGrow={1} minHeight="300px">
            <TaskLogContent
              error={error}
              expanded
              isLoading={isLoading}
              logError={error}
              parsedLogs={parsedLogs}
              wrap
            />
          </Box>
        )}
      </Modal>
    </>
  );
};
