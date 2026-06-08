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
import { Badge, Box, Heading, HStack, Link, List, Skeleton, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiAlertCircle } from "react-icons/fi";

import { Alert } from "src/components/ui";
import { useTriageSummary } from "src/queries/useTriageSummary";

export type TriagePanelProps = {
  readonly dagId: string;
  readonly mapIndex: number;
  readonly runId: string;
  readonly taskId: string;
};

const formatCategoryLabel = (name: string) => name.replaceAll("_", " ");

export const TriagePanel = ({ dagId, mapIndex, runId, taskId }: TriagePanelProps) => {
  const { t: translate } = useTranslation("dag");
  const { data, isLoading } = useTriageSummary({
    dagId,
    mapIndex,
    runId,
    taskId,
  });

  return (
    <Box
      bg="bg.panel"
      borderColor="border.emphasized"
      borderRadius="md"
      borderWidth="1px"
      data-testid="triage-panel"
      mb={3}
      p={4}
    >
      <Heading mb={3} size="md">
        {translate("triage.title")}
      </Heading>

      {isLoading ? (
        <VStack align="stretch" gap={2}>
          <Skeleton height="20px" width="60%" />
          <Skeleton height="16px" width="100%" />
          <Skeleton height="16px" width="90%" />
          <Skeleton height="80px" width="100%" />
        </VStack>
      ) : data === null || data === undefined ? (
        <Text color="fg.muted" fontSize="sm">
          {translate("triage.unavailable")}
        </Text>
      ) : (
        <VStack align="stretch" gap={4}>
          {data.categories.length > 0 ? (
            <Box>
              <Text fontSize="sm" fontWeight="semibold" mb={2}>
                {translate("triage.failureCategory")}
              </Text>
              <HStack flexWrap="wrap" gap={2}>
                {data.categories.map((category) => (
                  <Badge colorPalette="red" key={category.name} variant="surface">
                    {formatCategoryLabel(category.name)} ({Math.round(category.confidence * 100)}%)
                  </Badge>
                ))}
              </HStack>
            </Box>
          ) : undefined}

          <Box>
            <Text fontSize="sm" fontWeight="semibold" mb={2}>
              {translate("triage.rootCauseSummary")}
            </Text>
            <Text fontSize="sm">{data.root_cause_summary}</Text>
          </Box>

          {data.remediations.length > 0 ? (
            <Box>
              <Text fontSize="sm" fontWeight="semibold" mb={2}>
                {translate("triage.remediationChecklist")}
              </Text>
              <VStack align="stretch" gap={3}>
                {data.remediations.map((remediation) => (
                  <Box key={remediation.title}>
                    <Text fontSize="sm" fontWeight="medium" mb={1}>
                      {remediation.title}
                    </Text>
                    <List.Root fontSize="sm" gap={1} ml={4} variant="plain">
                      {remediation.steps.map((step) => (
                        <List.Item key={step}>{step}</List.Item>
                      ))}
                    </List.Root>
                    {remediation.doc_links.length > 0 ? (
                      <HStack flexWrap="wrap" gap={2} mt={2}>
                        {remediation.doc_links.map((docLink) => (
                          <Link colorPalette="blue" fontSize="sm" href={docLink} key={docLink} target="_blank">
                            {translate("triage.documentationLink")}
                          </Link>
                        ))}
                      </HStack>
                    ) : undefined}
                  </Box>
                ))}
              </VStack>
            </Box>
          ) : undefined}

          {data.error ? (
            <Alert icon={<FiAlertCircle />} status="warning" title={translate("triage.partialDataTitle")}>
              {data.error}
            </Alert>
          ) : undefined}
        </VStack>
      )}
    </Box>
  );
};
