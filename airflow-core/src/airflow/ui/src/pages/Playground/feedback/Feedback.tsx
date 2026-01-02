/* eslint-disable i18next/no-literal-string */

/* eslint-disable react/jsx-max-depth */

/* eslint-disable @typescript-eslint/no-unsafe-assignment */

/* eslint-disable @typescript-eslint/no-explicit-any */

/* eslint-disable max-lines */

 

 

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
import {
  Box,
  Collapsible,
  Heading,
  HStack,
  Progress,
  ProgressCircle,
  Spinner,
  Text,
  VStack,
  Alert,
} from "@chakra-ui/react";

import { StateBadge } from "src/components/StateBadge";
import { StateIcon } from "src/components/StateIcon";

type FeedbackProps = {
  readonly isProgressOpen: boolean;
  readonly isStatesOpen: boolean;
  readonly onProgressToggle: () => void;
  readonly onStatesToggle: () => void;
  readonly progressValue: number;
  readonly setProgressValue: (value: number) => void;
};

export const Feedback = ({
  isProgressOpen,
  isStatesOpen,
  onProgressToggle,
  onStatesToggle,
  progressValue,
}: FeedbackProps) => {
  const taskStates = [
    { count: 150, label: "Success", state: "success" },
    { count: 25, label: "Running", state: "running" },
    { count: 8, label: "Failed", state: "failed" },
    { count: 3, label: "Upstream Failed", state: "upstream_failed" },
    { count: 45, label: "Skipped", state: "skipped" },
    { count: 5, label: "Up for Retry", state: "up_for_retry" },
    { count: 2, label: "Up for Reschedule", state: "up_for_reschedule" },
    { count: 12, label: "Queued", state: "queued" },
    { count: 30, label: "Scheduled", state: "scheduled" },
    { count: 7, label: "Deferred", state: "deferred" },
    { count: 1, label: "Removed", state: "removed" },
  ];

  return (
    <VStack align="stretch" gap={8}>
      {/* Progress & Alerts Section */}
      <Box id="progress">
        <Collapsible.Root onOpenChange={onProgressToggle} open={isProgressOpen}>
          <Collapsible.Trigger
            _hover={{ bg: "bg.subtle" }}
            borderColor={isProgressOpen ? "brand.emphasized" : "border.muted"}
            borderWidth="1px"
            cursor="pointer"
            paddingX="6"
            paddingY="4"
            transition="all 0.2s"
            width="full"
          >
            <HStack justify="space-between" width="full">
              <VStack align="flex-start" gap="1">
                <Heading size="xl">Progress & Alerts</Heading>
                <Text color="fg.muted" fontSize="sm">
                  Progress indicators, loading states, and alert messages
                </Text>
              </VStack>
              <Text color="brand.solid" fontSize="lg">
                {isProgressOpen ? "−" : "+"}
              </Text>
            </HStack>
          </Collapsible.Trigger>
          <Collapsible.Content>
            <Box borderColor="border.muted" borderTop="none" borderWidth="1px" padding="6">
              <VStack align="stretch" gap={8}>
                {/* Linear Progress */}
                <VStack align="stretch" gap={4}>
                  <Heading size="lg">Linear Progress</Heading>
                  <VStack align="stretch" gap={4}>
                    <Box>
                      <Text fontSize="sm" fontWeight="semibold">
                        Default Progress ({progressValue}%)
                      </Text>
                      <Progress.Root value={progressValue}>
                        <Progress.Track>
                          <Progress.Range />
                        </Progress.Track>
                      </Progress.Root>
                    </Box>

                    <Box>
                      <Text fontSize="sm" fontWeight="semibold">
                        Colored Progress
                      </Text>
                      <VStack align="stretch" gap={2}>
                        <Progress.Root colorPalette="green" value={85}>
                          <Progress.Track>
                            <Progress.Range />
                          </Progress.Track>
                        </Progress.Root>
                        <Progress.Root colorPalette="yellow" value={60}>
                          <Progress.Track>
                            <Progress.Range />
                          </Progress.Track>
                        </Progress.Root>
                        <Progress.Root colorPalette="red" value={30}>
                          <Progress.Track>
                            <Progress.Range />
                          </Progress.Track>
                        </Progress.Root>
                      </VStack>
                    </Box>
                  </VStack>
                </VStack>

                {/* Circular Progress */}
                <VStack align="stretch" gap={4}>
                  <Heading size="lg">Circular Progress</Heading>
                  <HStack gap={6} wrap="wrap">
                    <VStack gap={2}>
                      <ProgressCircle.Root size="lg" value={progressValue}>
                        <ProgressCircle.Circle>
                          <ProgressCircle.Track />
                          <ProgressCircle.Range />
                        </ProgressCircle.Circle>
                        <ProgressCircle.ValueText />
                      </ProgressCircle.Root>
                      <Text fontSize="sm">Default</Text>
                    </VStack>

                    <VStack gap={2}>
                      <ProgressCircle.Root colorPalette="green" size="lg" value={85}>
                        <ProgressCircle.Circle>
                          <ProgressCircle.Track />
                          <ProgressCircle.Range />
                        </ProgressCircle.Circle>
                        <ProgressCircle.ValueText />
                      </ProgressCircle.Root>
                      <Text fontSize="sm">Success</Text>
                    </VStack>

                    <VStack gap={2}>
                      <ProgressCircle.Root colorPalette="red" size="lg" value={30}>
                        <ProgressCircle.Circle>
                          <ProgressCircle.Track />
                          <ProgressCircle.Range />
                        </ProgressCircle.Circle>
                        <ProgressCircle.ValueText />
                      </ProgressCircle.Root>
                      <Text fontSize="sm">Error</Text>
                    </VStack>
                  </HStack>
                </VStack>

                {/* Spinners */}
                <VStack align="stretch" gap={4}>
                  <Heading size="lg">Loading Spinners</Heading>
                  <HStack gap={6} wrap="wrap">
                    <VStack gap={2}>
                      <Spinner size="xs" />
                      <Text fontSize="sm">Extra Small</Text>
                    </VStack>
                    <VStack gap={2}>
                      <Spinner size="sm" />
                      <Text fontSize="sm">Small</Text>
                    </VStack>
                    <VStack gap={2}>
                      <Spinner size="md" />
                      <Text fontSize="sm">Medium</Text>
                    </VStack>
                    <VStack gap={2}>
                      <Spinner size="lg" />
                      <Text fontSize="sm">Large</Text>
                    </VStack>
                    <VStack gap={2}>
                      <Spinner size="xl" />
                      <Text fontSize="sm">Extra Large</Text>
                    </VStack>
                  </HStack>
                </VStack>

                {/* Alerts */}
                <VStack align="stretch" gap={4}>
                  <Heading size="lg">Alerts & Notifications</Heading>
                  <VStack align="stretch" gap={4}>
                    {/* Success Alert */}
                    <Alert.Root colorPalette="green">
                      <Alert.Indicator />
                      <VStack align="flex-start" gap={1}>
                        <Alert.Title>Success!</Alert.Title>
                        <Alert.Description>
                          Your DAG has been successfully deployed and is now running.
                        </Alert.Description>
                      </VStack>
                    </Alert.Root>

                    {/* Warning Alert */}
                    <Alert.Root colorPalette="yellow">
                      <Alert.Indicator />
                      <VStack align="flex-start" gap={1}>
                        <Alert.Title>Warning</Alert.Title>
                        <Alert.Description>
                          This DAG has been running for more than 24 hours. Consider checking for issues.
                        </Alert.Description>
                      </VStack>
                    </Alert.Root>

                    {/* Error Alert */}
                    <Alert.Root colorPalette="red">
                      <Alert.Indicator />
                      <VStack align="flex-start" gap={1}>
                        <Alert.Title>Error</Alert.Title>
                        <Alert.Description>
                          Failed to connect to the database. Please check your connection settings.
                        </Alert.Description>
                      </VStack>
                    </Alert.Root>

                    {/* Info Alert */}
                    <Alert.Root colorPalette="blue">
                      <Alert.Indicator />
                      <VStack align="flex-start" gap={1}>
                        <Alert.Title>Information</Alert.Title>
                        <Alert.Description>
                          New features are available in the latest version. Consider updating your installation.
                        </Alert.Description>
                      </VStack>
                    </Alert.Root>
                  </VStack>
                </VStack>
              </VStack>
            </Box>
          </Collapsible.Content>
        </Collapsible.Root>
      </Box>
    </VStack>
  );
};
